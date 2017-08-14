package mobius

import (
	"encoding/json"
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/montanaflynn/stats"
	"regexp"
	"sort"
	"strings"
	"time"
)

var rxMetricNextTag = regexp.MustCompile(`(?:[^=]+)=(?:[^=]+)(,|$)`)
var NameTagsDelimiter = `:`
var InlineTagSeparator = `,`

type Metric struct {
	Metadata map[string]interface{} `json:"metadata,omitempty"`
	MaxSize  int
	name     string
	tags     map[string]interface{}
	points   PointSet
}

func NewMetric(name string) *Metric {
	metric := &Metric{
		Metadata: make(map[string]interface{}),
		points:   make(PointSet, 0),
	}

	metric.SetName(name)

	return metric
}

func (self *Metric) GetName() string {
	return self.name
}

func (self *Metric) Reset() PointSet {
	oldPoints := make(PointSet, len(self.points))
	copy(oldPoints, self.points)
	self.points = make(PointSet, 0)
	return oldPoints
}

func (self *Metric) SetName(name string) {
	name, tags := SplitNameTags(name)
	self.name = name
	self.tags = tags
}

func (self *Metric) GetTags() map[string]interface{} {
	return self.tags
}

func (self *Metric) GetTag(key string) interface{} {
	if v, ok := self.tags[key]; ok {
		return v
	}

	return nil
}

func (self *Metric) SetTags(tags map[string]interface{}) {
	self.tags = tags
}

func (self *Metric) SetTag(key string, value interface{}) {
	self.tags[key] = value
}

func (self *Metric) Points() PointSet {
	return self.points
}

func (self *Metric) IsEmpty() bool {
	if len(self.points) == 0 {
		return true
	}

	return false
}

func (self *Metric) GetUniqueName() string {
	name := self.name
	keys := maputil.StringKeys(self.tags)
	sort.Strings(keys)
	tagset := make([]string, len(keys))

	for i, tag := range keys {
		if value, ok := self.tags[tag]; ok && !typeutil.IsEmpty(value) {
			if typeutil.IsArray(value) {
				tagset[i] = fmt.Sprintf("%s=%v", tag, strings.Join(sliceutil.Stringify(value), `|`))
			} else {
				tagset[i] = fmt.Sprintf("%s=%v", tag, value)
			}
		}
	}

	if len(tagset) > 0 {
		name = name + NameTagsDelimiter + strings.Join(tagset, `,`)
	}

	return name
}

func (self *Metric) PushPoint(point Point) *Metric {
	return self.Push(point.Timestamp, point.Value)
}

func (self *Metric) Push(timestamp time.Time, value float64) *Metric {
	self.points = append(self.points, Point{
		Timestamp: timestamp,
		Value:     value,
	})

	// if we've exceeded MaxSize, shift the points slice such that it only includes the
	// most recent <MaxSize> elements.
	if l := len(self.points); self.MaxSize > 0 && l > self.MaxSize {
		self.points = self.points[(l - self.MaxSize):]
	}

	return self
}

// Returns the value that represents a given standing within this metric's data.
func (self *Metric) Percentile(percent float64) (float64, error) {
	return stats.Percentile(stats.Float64Data(self.points.Values()), percent)
}

// Describes the degree of relationship between this metric and another one.
func (self *Metric) Correlation(other *Metric) (float64, error) {
	return stats.Correlation(
		stats.Float64Data(self.points.Values()),
		stats.Float64Data(other.points.Values()),
	)
}

// Covariance is a measure of how much this metric changes with respect to another.
func (self *Metric) Covariance(other *Metric) (float64, error) {
	return stats.Covariance(
		stats.Float64Data(self.points.Values()),
		stats.Float64Data(other.points.Values()),
	)
}

func (self *Metric) CovariancePopulation(other *Metric) (float64, error) {
	return stats.CovariancePopulation(
		stats.Float64Data(self.points.Values()),
		stats.Float64Data(other.points.Values()),
	)
}

func (self *Metric) Summarize(reducers ...ReducerFunc) []float64 {
	return SummarizeMetric(self, reducers...)
}

func (self *Metric) MarshalJSON() ([]byte, error) {
	rv := map[string]interface{}{
		`name`:        self.GetName(),
		`unique_name`: self.GetUniqueName(),
	}

	if v := self.GetTags(); len(v) > 0 {
		rv[`tags`] = v
	}

	if v, err := maputil.Compact(self.Metadata); err == nil {
		if len(v) > 0 {
			rv[`metadata`] = v
		}
	} else {
		return nil, err
	}

	if v := self.Points(); len(v) > 0 {
		rv[`points`] = v
	}

	return json.Marshal(rv)
}

func (self *Metric) Consolidate(size time.Duration, reducer ReducerFunc) *Metric {
	return ConsolidateMetric(self, size, reducer)
}

func SplitNameTags(name string) (string, map[string]interface{}) {
	tags := make(map[string]interface{})
	parts := strings.SplitN(name, NameTagsDelimiter, 2)
	name = parts[0]

	if len(parts) > 1 {
		for _, match := range rxMetricNextTag.FindAllString(parts[1], -1) {
			pair := strings.Trim(string(match[:]), `,`)
			kv := strings.SplitN(pair, `=`, 2)

			if len(kv) == 2 {
				values := make([]interface{}, 0)

				for _, tagval := range strings.Split(kv[1], `|`) {
					values = append(values, stringutil.Autotype(tagval))
				}

				switch len(values) {
				case 0:
					continue
				case 1:
					tags[kv[0]] = values[0]
				default:
					tags[kv[0]] = values
				}
			}
		}
	}

	return name, tags
}

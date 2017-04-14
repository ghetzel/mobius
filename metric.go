package mobius

import (
	"encoding/json"
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"sort"
	"strings"
	"time"
)

var InlineTagSeparator = `,`

type IMetric interface {
	SetName(string)
	GetName() string
	GetUniqueName() string
	GetTag(string) interface{}
	GetTags() map[string]interface{}
	SetTag(string, interface{})
	Points() PointSet
	Push(time.Time, float64)
	Reset() PointSet
	Consolidate(time.Duration, ReducerFunc) IMetric
}

type Metric struct {
	IMetric
	name   string
	tags   map[string]interface{}
	points PointSet
}

func NewMetric(name string) *Metric {
	metric := &Metric{
		points: make(PointSet, 0),
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
	name, tags := SplitNameTags(name, InlineTagSeparator)
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

func (self *Metric) SetTag(key string, value interface{}) {
	self.tags[key] = value
}

func (self *Metric) Points() PointSet {
	return self.points
}

func (self *Metric) GetUniqueName() string {
	name := self.name
	keys := maputil.StringKeys(self.tags)
	sort.Strings(keys)

	for _, tag := range keys {
		if value, ok := self.tags[tag]; ok && !typeutil.IsEmpty(value) {
			name += fmt.Sprintf("%s%s=%v", InlineTagSeparator, tag, value)
		}
	}

	return name
}

func (self *Metric) Push(timestamp time.Time, value float64) {
	self.points = append(self.points, Point{
		Timestamp: timestamp,
		Value:     value,
	})
}

func (self *Metric) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		`name`:        self.GetName(),
		`unique_name`: self.GetUniqueName(),
		`tags`:        self.GetTags(),
		`points`:      self.Points(),
	})
}

func (self *Metric) Consolidate(size time.Duration, reducer ReducerFunc) IMetric {
	return ConsolidateMetric(self, size, reducer)
}

func SplitNameTags(name string, sep string) (string, map[string]interface{}) {
	tags := make(map[string]interface{})
	parts := strings.Split(name, sep)

	if len(parts) > 1 {
		for _, pair := range parts[1:] {
			kv := strings.SplitN(pair, `=`, 2)

			if len(kv) == 2 {
				tags[kv[0]] = stringutil.Autotype(kv[1])
			}
		}

		name = parts[0]
	}

	return name, tags
}

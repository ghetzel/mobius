package mobius

import (
	"encoding/binary"
	"fmt"
	"github.com/gobwas/glob"
	"github.com/op/go-logging"
	"github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/ledis"
	"math"
	"strings"
	"time"
)

var log = logging.MustGetLogger(`mobius/db`)

var MetricValuePattern = "mobius:metrics:%s:values"
var MetricNameSetKey = "mobius:metrics:names"

type Dataset struct {
	StoreZeroes bool
	directory   string
	conn        *ledis.Ledis
	db          *ledis.DB
}

func OpenDataset(directory string) (*Dataset, error) {
	c := config.NewConfigDefault()

	c.DataDir = directory

	if conn, err := ledis.Open(c); err == nil {
		if db, err := conn.Select(0); err == nil {
			return &Dataset{
				directory: directory,
				conn:      conn,
				db:        db,
			}, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func (self *Dataset) Close() error {
	if self.conn != nil {
		self.conn.Close()
	}

	return nil
}

func (self *Dataset) GetNames(pattern string) ([]string, error) {
	if matcher, err := glob.Compile(pattern, '.'); err == nil {
		names := make([]string, 0)

		if nameset, err := self.db.SMembers([]byte(MetricNameSetKey)); err == nil {
			for _, member := range nameset {
				if name := string(member[:]); strings.HasPrefix(name, pattern+InlineTagSeparator) || matcher.Match(name) {
					names = append(names, name)
				}
			}
		} else {
			return nil, err
		}

		return names, nil
	} else {
		return nil, err
	}
}

func (self *Dataset) Range(start time.Time, end time.Time, names ...string) ([]IMetric, error) {
	metrics := make([]IMetric, 0)
	var startZScore, endZScore int64

	if !start.IsZero() {
		startZScore = start.UnixNano()
	}

	if end.IsZero() {
		endZScore = time.Now().UnixNano()
	} else {
		endZScore = end.UnixNano()
	}

	for _, nameset := range names {
		if expandedNames, err := self.GetNames(nameset); err == nil {
			for _, name := range expandedNames {
				metric := NewMetric(name)
				metricValueKey := []byte(fmt.Sprintf(MetricValuePattern, name))

				if results, err := self.db.ZRangeByScore(metricValueKey, startZScore, endZScore, 0, -1); err == nil {
					for _, result := range results {
						metric.Push(&Point{
							Timestamp: time.Unix(0, result.Score),
							Value:     bytesToFloat(result.Member),
						})
					}
				} else {
					return nil, err
				}

				metrics = append(metrics, metric)
			}
		} else {
			return nil, err
		}
	}

	return metrics, nil
}

func (self *Dataset) Write(metric IMetric, point *Point) error {
	if metric != nil && point != nil {
		if self.StoreZeroes || point.Value != 0 {
			metricName := metric.GetUniqueName()
			metricValueKey := []byte(fmt.Sprintf(MetricValuePattern, metricName))

			// write the metric name to a set to allow name pattern matching
			if _, err := self.db.SAdd([]byte(MetricNameSetKey), []byte(metricName)); err != nil {
				return fmt.Errorf("name index failed: %v", err)
			}

			// write the value to a sorted set keyed on metric name, scored on epoch nano
			if _, err := self.db.ZAdd(metricValueKey, ledis.ScorePair{
				Score:  point.Timestamp.UnixNano(),
				Member: floatToBytes(point.Value),
			}); err != nil {
				return fmt.Errorf("write failed: %v", err)
			}
		}
	}

	return nil
}

func floatToBytes(in float64) []byte {
	out := make([]byte, 8)
	ieee754 := math.Float64bits(in)
	binary.BigEndian.PutUint64(out, ieee754)
	return out
}

func bytesToFloat(in []byte) float64 {
	bits := binary.BigEndian.Uint64(in)
	out := math.Float64frombits(bits)
	return out
}

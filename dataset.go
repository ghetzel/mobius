package mobius

import (
	"encoding/binary"
	"fmt"
	"github.com/gobwas/glob"
	"github.com/op/go-logging"
	"github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/ledis"
	"io"
	"math"
	"sort"
	"strings"
	"time"
)

var log = logging.MustGetLogger(`mobius/db`)

var MetricValuePattern = "mobius:metrics:%s:values"
var MetricRangePattern = "mobius:metrics:%s:range"
var MetricNameSetKey = "mobius:metrics:names"

type trimDirection int

const (
	trimBeforeMark trimDirection = iota
	trimAfterMark
)

type Dataset struct {
	StoreZeroes bool
	directory   string
	conn        *ledis.Ledis
	db          *ledis.DB
}

func OpenDataset(directory string) (*Dataset, error) {
	return openDataset(directory, false)
}

func OpenDatasetReadOnly(directory string) (*Dataset, error) {
	return openDataset(directory, true)
}

func openDataset(directory string, readonly bool) (*Dataset, error) {
	c := config.NewConfigDefault()

	c.DataDir = directory
	c.SetReadonly(readonly)

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

func (self *Dataset) GetPath() string {
	return self.directory
}

func (self *Dataset) Close() error {
	if self.conn != nil {
		self.conn.Close()
	}

	return nil
}

func (self *Dataset) Compact() error {
	return self.conn.CompactStore()
}

func (self *Dataset) Backup(w io.Writer) error {
	return self.conn.Dump(w)
}

func (self *Dataset) Restore(r io.Reader) error {
	_, err := self.conn.LoadDump(r)
	return err
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

		sort.Strings(names)

		return names, nil
	} else {
		return nil, err
	}
}

func (self *Dataset) Range(start time.Time, end time.Time, names ...string) ([]*Metric, error) {
	metrics := make([]*Metric, 0)
	var startZScore, endZScore int64

	if !start.IsZero() {
		startZScore = start.UnixNano()
	} else {
		startZScore = math.MinInt64
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
				metricRangeKey := []byte(fmt.Sprintf(MetricRangePattern, name))

				if results, err := self.db.ZRangeByScore(metricRangeKey, startZScore, endZScore, 0, -1); err == nil {
					for _, result := range results {
						if value, err := self.db.HGet(metricValueKey, result.Member); err == nil {
							metric.Push(time.Unix(0, result.Score), bytesToFloat(value))
						} else {
							log.Warningf("Value at key %v[%v] missing", string(metricValueKey[:]), result.Score)
						}
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

func (self *Dataset) Write(metric *Metric) error {
	if metric != nil {
		metricName := metric.GetUniqueName()
		metricValueKey := []byte(fmt.Sprintf(MetricValuePattern, metricName))
		metricRangeKey := []byte(fmt.Sprintf(MetricRangePattern, metricName))

		// write the metric name to a set to allow name pattern matching
		if _, err := self.db.SAdd([]byte(MetricNameSetKey), []byte(metricName)); err != nil {
			return fmt.Errorf("name index failed: %v", err)
		}

		for _, point := range metric.Points() {
			if self.StoreZeroes || point.Value != 0 {
				epoch := point.Timestamp.UnixNano()
				epochBytes := int64ToBytes(epoch)

				// write the value to hash at metric name, keyed on epoch nano
				if _, err := self.db.HSet(
					metricValueKey,
					epochBytes,
					floatToBytes(point.Value),
				); err != nil {
					return fmt.Errorf("write failed: %v", err)
				}

				// add the time to a sorted set for this metric for efficient ranging
				if _, err := self.db.ZAdd(metricRangeKey, ledis.ScorePair{
					Score:  epoch,
					Member: epochBytes,
				}); err != nil {
					defer self.db.HDel(metricValueKey)
					return fmt.Errorf("write failed: %v", err)
				}
			}
		}
	}

	return nil
}

func (self *Dataset) Remove(names ...string) (int64, error) {
	var totalRemoved int64
	valueKeysToClear := make([][]byte, 0)
	namesToClear := make([][]byte, 0)

	for _, nameset := range names {
		if expandedNames, err := self.GetNames(nameset); err == nil {
			for _, name := range expandedNames {
				namesToClear = append(
					namesToClear,
					[]byte(name),
				)

				valueKeysToClear = append(
					valueKeysToClear,
					[]byte(fmt.Sprintf(MetricValuePattern, name)),
				)
			}

			if n, err := self.db.SRem([]byte(MetricNameSetKey), namesToClear...); err == nil {
				log.Debugf("Removed %d metric names", n)
			} else {
				log.Errorf("Failed to remove metric names: %v", err)
			}

			return self.db.ZMclear(valueKeysToClear...)
		} else {
			return totalRemoved, err
		}
	}

	return 0, nil
}

func (self *Dataset) TrimBefore(before time.Time, names ...string) (int64, error) {
	return self.trim(trimBeforeMark, before, names...)
}

func (self *Dataset) TrimAfter(after time.Time, names ...string) (int64, error) {
	return self.trim(trimAfterMark, after, names...)
}

func (self *Dataset) trim(direction trimDirection, mark time.Time, names ...string) (int64, error) {
	var start int64
	var end int64
	var totalRemoved int64

	switch direction {
	case trimBeforeMark:
		start = math.MinInt64
		end = (mark.UnixNano() - 1)
	case trimAfterMark:
		start = mark.UnixNano()
		end = math.MaxInt64
	}

	if len(names) == 0 {
		names = []string{`**`}
	}

	for _, nameset := range names {
		if expandedNames, err := self.GetNames(nameset); err == nil {
			for _, name := range expandedNames {
				metricValueKey := []byte(fmt.Sprintf(MetricValuePattern, name))
				metricRangeKey := []byte(fmt.Sprintf(MetricRangePattern, name))

				// need to get the keys we're trimming from the values hash
				if results, err := self.db.ZRangeByScore(metricRangeKey, start, end, 0, -1); err == nil {
					for _, result := range results {
						if _, err := self.db.HDel(metricValueKey, result.Member); err != nil {
							return totalRemoved, err
						}
					}
				} else {
					return totalRemoved, err
				}

				if n, err := self.db.ZRemRangeByScore(metricRangeKey, start, end); err == nil {
					totalRemoved += n
				} else {
					return totalRemoved, err
				}
			}
		} else {
			return totalRemoved, err
		}
	}

	return totalRemoved, nil
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

func int64ToBytes(in int64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, uint64(in))
	return out
}

func bytesToInt64(in []byte) int64 {
	return int64(binary.BigEndian.Uint64(in))
}

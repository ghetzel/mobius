package mobius

import (
	"encoding/binary"
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/jbenet/go-base58"
	"github.com/op/go-logging"
	"github.com/siddontang/ledisdb/config"
	"github.com/siddontang/ledisdb/ledis"
	"io"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
)

var log = logging.MustGetLogger(`mobius/db`)

var MetricValuePattern = "mobius:metrics:%s:values"
var MetricRangePattern = "mobius:metrics:%s:range"
var TagSetPattern = "mobius:tags:%s:%s"
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
	parts := strings.SplitN(pattern, NameTagsDelimiter, 2)

	pattern = parts[0]
	pattern = `^` + strings.TrimPrefix(pattern, `^`)
	pattern = strings.Replace(pattern, `.`, `\.`, -1)
	pattern = strings.Replace(pattern, `*`, `[^\.]*`, -1)
	pattern = strings.Replace(pattern, `**`, `.*`, -1)
	pattern = strings.Replace(pattern, `?`, `.`, -1)
	requiredTags := make(map[string][]string)

	if len(parts) == 2 {
		for k, v := range maputil.Split(parts[1], `=`, InlineTagSeparator) {
			requiredTags[k] = strings.Split(fmt.Sprintf("%v", v), `|`)
		}
	}

	if matcher, err := regexp.Compile(pattern); err == nil {
		names := make([]string, 0)

		if nameset, err := self.db.SMembers([]byte(MetricNameSetKey)); err == nil {
		NameLoop:
			for _, member := range nameset {
				if name := string(member[:]); strings.HasPrefix(name, pattern+InlineTagSeparator) || matcher.MatchString(name) {
					// if tag filters were given, skip this iteration if the current metric name
					// does not appear in all of the associated tagsets
					for tag, values := range requiredTags {
						shouldSkip := true

						for _, value := range values {
							if self.IsTagValueInName(name, tag, value) {
								shouldSkip = false
								break
							}
						}

						if shouldSkip {
							continue NameLoop
						}
					}

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

func (self *Dataset) IsTagValueInName(name string, tag string, value interface{}) bool {
	if n, err := self.db.SIsMember([]byte(tagSetKey(tag, value)), []byte(name)); err == nil && n > 0 {
		return true
	} else {
		return false
	}
}

// func (self *Dataset) GetNamesForTag(tag string, value string) ([]string, error) {

// }

func (self *Dataset) Oldest(names ...string) ([]*Metric, error) {
	return self.rangeGeneric(time.Time{}, time.Time{}, 1, false, names...)
}

func (self *Dataset) Newest(names ...string) ([]*Metric, error) {
	return self.rangeGeneric(time.Time{}, time.Time{}, 1, true, names...)
}

func (self *Dataset) Range(start time.Time, end time.Time, names ...string) ([]*Metric, error) {
	return self.rangeGeneric(start, end, -1, false, names...)
}

func (self *Dataset) rangeGeneric(start time.Time, end time.Time, maxPointsPerMetric int, reverse bool, names ...string) ([]*Metric, error) {
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

				if results, err := self.db.ZRangeByScoreGeneric(metricRangeKey, startZScore, endZScore, 0, maxPointsPerMetric, reverse); err == nil {
					// if we traversed the range in reverse order, we need to reverse the results so they are ordered as time-ascending
					if reverse {
						sort.Slice(results, func(i, j int) bool {
							return results[i].Score > results[j].Score
						})
					}

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

		// write the metric name to hashes for each tag value
		for tag, value := range metric.GetTags() {
			if _, err := self.db.SAdd([]byte(tagSetKey(tag, value)), []byte(metricName)); err != nil {
				return fmt.Errorf("tag index failed: %v", err)
			}
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

		if err := self.TrimOldestToCount(metric.MaxSize, metricName); err != nil {
			return err
		}
	}

	return nil
}

func (self *Dataset) Remove(names ...string) (int64, error) {
	var totalRemoved int64
	valueKeysToClear := make([][]byte, 0)
	namesToClear := make([][]byte, 0)
	tagsToClear := make(map[string][][]byte, 0)

	for _, nameset := range names {
		if expandedNames, err := self.GetNames(nameset); err == nil {
			for _, name := range expandedNames {
				metric := NewMetric(name)

				// add this metric name to the list being removed from the Metric Name Set
				namesToClear = append(
					namesToClear,
					[]byte(metric.GetUniqueName()),
				)

				// add this metric name to the list being removed from the Metric Value Hash
				valueKeysToClear = append(
					valueKeysToClear,
					[]byte(fmt.Sprintf(MetricValuePattern, metric.GetUniqueName())),
				)

				// add this metric name to the list being removed from the Tag Value Set
				for tag, value := range metric.GetTags() {
					tsKey := tagSetKey(tag, value)

					if _, ok := tagsToClear[tsKey]; !ok {
						tagsToClear[tsKey] = make([][]byte, 0)
					}

					if clear, ok := tagsToClear[tsKey]; ok {
						tagsToClear[tsKey] = append(clear, []byte(metric.GetUniqueName()))
					}
				}
			}

			// remove unique names from name set
			if n, err := self.db.SRem([]byte(MetricNameSetKey), namesToClear...); err == nil {
				log.Debugf("Removed %d metric names", n)
			} else {
				log.Errorf("Failed to remove metric names: %v", err)
			}

			// remove names from tag set
			for tagset, keys := range tagsToClear {
				if n, err := self.db.SRem([]byte(tagset), keys...); err == nil {
					log.Debugf("Removed %d metrics from the %s tagset", n, tagset)
				} else {
					log.Errorf("Failed to remove metrics from the %s tagset: %v", tagset, err)
				}
			}

			return self.db.ZMclear(valueKeysToClear...)
		} else {
			return totalRemoved, err
		}
	}

	return 0, nil
}

func (self *Dataset) NumPoints(nameGlob string) int {
	return self.numPointsGeneric(nameGlob, math.MinInt64, math.MaxInt64)
}

func (self *Dataset) NumPointsRange(nameGlob string, start time.Time, end time.Time) int {
	startInt := math.MinInt64
	endInt := math.MaxInt64

	if !start.IsZero() {
		startInt = int(start.UnixNano())
	}

	if !end.IsZero() {
		endInt = int(end.UnixNano() - 1)
	}

	return self.numPointsGeneric(nameGlob, startInt, endInt)
}

func (self *Dataset) numPointsGeneric(nameset string, start int, end int) int {
	var count int64

	if expandedNames, err := self.GetNames(nameset); err == nil {
		for _, name := range expandedNames {
			metricRangeKey := []byte(fmt.Sprintf(MetricRangePattern, name))

			if c, err := self.db.ZCount(metricRangeKey, math.MinInt64, math.MaxInt64); err == nil {
				count += c
			}
		}
	}

	return int(count)
}

func (self *Dataset) TrimBefore(before time.Time, names ...string) (int64, error) {
	return self.trim(trimBeforeMark, before, names...)
}

func (self *Dataset) TrimAfter(after time.Time, names ...string) (int64, error) {
	return self.trim(trimAfterMark, after, names...)
}

// Removes the least-recent points that fall outside of the given Metric's Size (if Size is greater
// than zero.)
func (self *Dataset) TrimOldestToCount(size int, names ...string) error {
	return self.trimCountGeneric(size, false, names...)
}

// Removes the most-recent points that fall outside of the given Metric's Size (if Size is greater
// than zero.)
func (self *Dataset) TrimNewestToCount(size int, names ...string) error {
	return self.trimCountGeneric(size, true, names...)
}

func (self *Dataset) trimCountGeneric(toSize int, reverse bool, names ...string) error {
	if toSize > 0 {
		for _, nameset := range names {
			if expandedNames, err := self.GetNames(nameset); err == nil {
				for _, name := range expandedNames {
					metricValueKey := []byte(fmt.Sprintf(MetricValuePattern, name))
					metricRangeKey := []byte(fmt.Sprintf(MetricRangePattern, name))

					// if the point count exceeds the given limit, the difference is how many we need to remove
					if count := self.NumPoints(name); count > toSize {
						diff := (count - toSize - 1)

						// range over said points to figure out which ones we need to remove
						if pairs, err := self.db.ZRangeGeneric(metricRangeKey, 0, int(diff), reverse); err == nil {
							members := make([][]byte, len(pairs))

							for i, score := range pairs {
								members[i] = score.Member
							}

							// remove the times from the sorted time set
							if _, err := self.db.ZRem(metricRangeKey, members...); err != nil {
								return err
							}

							// remove the values from the map that holds values
							if _, err := self.db.HDel(metricValueKey, members...); err != nil {
								return err
							}
						} else {
							return err
						}
					}
				}
			} else {
				return err
			}
		}
	}

	return nil
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
					// remove the values from the metric
					for _, result := range results {
						if _, err := self.db.HDel(metricValueKey, result.Member); err != nil {
							return totalRemoved, err
						}
					}
				} else {
					return totalRemoved, err
				}

				// remove the value keys from the sorted set
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

func tagSetKey(tag string, value interface{}) string {
	valueBytes := []byte(fmt.Sprintf("%v", value))
	return fmt.Sprintf(TagSetPattern, tag, base58.Encode(valueBytes))
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

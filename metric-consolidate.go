package mobius

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/stringutil"
	"sort"
	"strings"
	"time"
)

// Divides the points contained in given Metric into buckets spanning a given duration of time, then applies
// a reducer function to the values in each bucket.  A new Metric is returned containing the consolidated
// points.
func ConsolidateMetric(inputMetric *Metric, bucketSize time.Duration, reducer ReducerFunc) *Metric {
	// clears the points out of the input metric, and returns a copy of the old PointSet
	metric := NewMetric(inputMetric.GetName())
	metric.SetTags(inputMetric.GetTags())

	// divide the old PointSet into buckets that are bucketSize wide
	for _, bucket := range MakeTimeBuckets(inputMetric.Points(), bucketSize) {
		// consolidate the bucket values according to the given reducer function
		consolidatedValue := Reduce(reducer, bucket.Values()...)

		// push the consolidated point to our metric
		metric.Push(bucket.Newest().Timestamp, consolidatedValue)
	}

	return metric
}

// Takes multiple input metrics and produces a set of metrics grouped by the given
// name or tag name, with all metrics in like groups being merged together such that all
// of the original points are in the same series.
func MergeMetrics(metrics []*Metric, groupBy string) []*Metric {
	output := make([]*Metric, 0)
	groupNamePairs := make(map[string]string)
	groupTags := make(map[string]map[string]interface{})
	groups := make(map[string][]*Metric)

	// split the input metrics into groups keyed on the field named in groupBy
	for _, metric := range metrics {
		var current []*Metric
		var currentGroup string

		switch groupBy {
		case `name`:
			currentGroup = metric.GetName()
		case `unique`:
			currentGroup = metric.GetUniqueName()
		default:
			if tagValue := metric.GetTag(groupBy); tagValue != nil {
				currentGroup = fmt.Sprintf("tag:%v:%v", groupBy, tagValue)
			} else {
				currentGroup = ``
			}
		}

		if c, ok := groups[currentGroup]; ok {
			current = c
		} else {
			current = make([]*Metric, 0)
		}

		current = append(current, metric)
		groups[currentGroup] = current
	}

	// figure out what the merged metric name and tags should look like for each group
	for group, groupMetrics := range groups {
		names := make([]string, 0)
		tags := make(map[string]interface{})

		for _, m := range groupMetrics {
			names = append(names, m.GetName())

			if t, err := maputil.Merge(tags, m.GetTags()); err == nil {
				tags = t
			}
		}

		groupNamePairs[group] = strings.Trim(stringutil.LongestCommonPrefix(names), `.,`)
		groupTags[group] = tags
	}

	// for each grouped metric, set the name, tags, and sort the points, then add to the output
	groupNames := maputil.StringKeys(groups)
	sort.Strings(groupNames)

	for _, group := range groupNames {
		if metrics, ok := groups[group]; ok {
			if v, ok := groupNamePairs[group]; ok {
				mergedMetric := NewMetric(v)

				if v, ok := groupTags[group]; ok {
					mergedMetric.SetTags(v)
				}

				for _, m := range metrics {
					mergedMetric.points = append(mergedMetric.points, m.points...)
				}

				if !mergedMetric.IsEmpty() {
					sort.Sort(mergedMetric.points)
					output = append(output, mergedMetric)
				}
			}
		}
	}

	return output
}

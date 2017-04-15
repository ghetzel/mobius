package mobius

import (
	"time"
	"sort"
)

func ConsolidateMetric(inputMetric *Metric, bucketSize time.Duration, reducer ReducerFunc) *Metric {
	// clears the points out of the input metric, and returns a copy of the old PointSet
	metric := NewMetric(inputMetric.GetUniqueName())

	// divide the old PointSet into buckets that are bucketSize wide
	for _, bucket := range MakeTimeBuckets(inputMetric.Points(), bucketSize) {
		// consolidate the bucket values according to the given reducer function
		consolidatedValue := Reduce(reducer, bucket.Values()...)

		// push the consolidated point to our metric
		metric.Push(bucket.Newest().Timestamp, consolidatedValue)
	}

	return metric
}

func MergeMetrics(metrics []*Metric) []*Metric {
	output := make([]*Metric, 0)
	byName := make(map[string]*Metric)

	for _, input := range metrics {
		var current *Metric

		if c, ok := byName[input.GetName()]; ok {
			current = c
		}else{
			current = NewMetric(input.GetName())
			byName[current.GetName()] = current
		}

		current.points = append(current.points, input.points...)
	}

	for _, metric := range byName {
		sort.Sort(metric.points)
		output = append(output, metric)
	}

	return output
}

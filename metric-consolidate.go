package mobius

import (
	"time"
)

func ConsolidateMetric(inputMetric IMetric, bucketSize time.Duration, reducer ReducerFunc) IMetric {
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

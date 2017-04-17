package mobius

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestMetricNameParse(t *testing.T) {
	assert := require.New(t)

	metric := NewMetric(`mobius.test.naming`)
	assert.Equal(`mobius.test.naming`, metric.GetName())
	assert.Equal(`mobius.test.naming`, metric.GetUniqueName())
	assert.Empty(metric.GetTags())

	metric = NewMetric(`mobius.test.naming,key=value,zzyxx=3.14,enabled=true`)
	assert.Equal(`mobius.test.naming`, metric.GetName())
	assert.Equal(`mobius.test.naming,enabled=true,key=value,zzyxx=3.14`, metric.GetUniqueName())
	assert.Equal(map[string]interface{}{
		`enabled`: true,
		`key`:     `value`,
		`zzyxx`:   3.14,
	}, metric.GetTags())

	metric = NewMetric(`mobius.test.naming,key=value,zzyxx={3.14,6.28},enabled=true`)
	assert.Equal(`mobius.test.naming`, metric.GetName())
	assert.Equal(`mobius.test.naming,enabled=true,key=value,zzyxx={3.14,6.28}`, metric.GetUniqueName())
	assert.Equal(map[string]interface{}{
		`enabled`: true,
		`key`:     `value`,
		`zzyxx`:   []interface{}{3.14, 6.28},
	}, metric.GetTags())
}

func TestMetricSummarize(t *testing.T) {
	assert := require.New(t)

	metric := NewMetric(`mobius.test.metrics.summarize`)

	last1 := float64(1)
	last2 := float64(1)

	for i := 0; i < 30; i++ {
		metric.Push(time.Date(2006, 1, 2, 15, 4, i, 0, mst), float64(last1))
		tmp := last2
		last2 = (last1 + last2)
		last1 = tmp
	}

	summary := SummarizeMetric(metric, Count, First, Last, Sum, Mean, Median, Minimum, Maximum, StandardDeviation)
	assert.Len(summary, 9)
	assert.Equal(float64(30), summary[0])
	assert.Equal(float64(1), summary[1])
	assert.Equal(float64(832040), summary[2])
	assert.Equal(float64(2178308), summary[3])
	assert.Equal(float64(72610.26666666666), summary[4])
	assert.Equal(float64(798.5), summary[5])
	assert.Equal(float64(1), summary[6])
	assert.Equal(float64(832040), summary[7])
	assert.Equal(float64(179070.01740453992), summary[8])
}

func TestMetricConsolidation(t *testing.T) {
	assert := require.New(t)

	metric := NewMetric(`mobius.test.metrics.consolidate`)

	for i := 0; i < 100; i++ {
		metric.Push(time.Date(2006, 1, 2, 15, 4, i, 0, mst), float64(i))
	}

	type reducerValues struct {
		Reducer ReducerFunc
		Values  []float64
	}

	for _, rv := range []reducerValues{
		{
			Reducer: Sum,
			Values:  []float64{45, 735, 1635, 2535},
		}, {
			Reducer: Minimum,
			Values:  []float64{0, 10, 40, 70},
		}, {
			Reducer: Maximum,
			Values:  []float64{9, 39, 69, 99},
		}, {
			Reducer: Mean,
			Values:  []float64{4.5, 24.5, 54.5, 84.5},
		},
	} {
		consolidated := metric.Consolidate(30*time.Second, rv.Reducer)
		points := consolidated.Points()
		assert.Len(points, 4)
		assert.Equal(time.Date(2006, 1, 2, 15, 4, 9, 0, mst), points[0].Timestamp)
		assert.Equal(time.Date(2006, 1, 2, 15, 4, 39, 0, mst), points[1].Timestamp)
		assert.Equal(time.Date(2006, 1, 2, 15, 5, 9, 0, mst), points[2].Timestamp)
		assert.Equal(time.Date(2006, 1, 2, 15, 5, 39, 0, mst), points[3].Timestamp)
		assert.Equal(rv.Values[0], points[0].Value)
		assert.Equal(rv.Values[1], points[1].Value)
		assert.Equal(rv.Values[2], points[2].Value)
		assert.Equal(rv.Values[3], points[3].Value)
	}
}

func TestMetricMerge(t *testing.T) {
	assert := require.New(t)

	metric0 := NewMetric(`mobius.test.metrics.merge,cool=beans,instance=1,class=onesies`)
	metric1 := NewMetric(`mobius.test.metrics.merge,cool=beans,instance=2,class=onesies`)
	metric2 := NewMetric(`mobius.test.metrics.merge,cool=beans,instance=3,class=twosies`)
	metric3 := NewMetric(`mobius.test.metrics.othermerge,other=1`)
	metric4 := NewMetric(`mobius.test.metrics.othermerge,other=2`)

	for i := 0; i < 35; i++ {
		metric0.Push(time.Date(2006, 1, 2, 15, 4, i, 0, mst), float64(i))
	}

	for i := 0; i < 100; i++ {
		metric1.Push(time.Date(2006, 1, 2, 15, 4, i, 0, mst), float64(i))
	}

	for i := 0; i < 100; i++ {
		metric2.Push(time.Date(2006, 1, 2, 15, 4, i, rand.Intn(100000), mst), float64(i))
	}

	for i := 0; i < 27; i++ {
		metric3.Push(time.Date(2006, 1, 2, 15, 4, i, rand.Intn(100000), mst), float64(i))
	}

	for i := 0; i < 28; i++ {
		metric4.Push(time.Date(2006, 1, 2, 15, 4, i, rand.Intn(100000), mst), float64(i))
	}

	merged := MergeMetrics([]*Metric{
		metric0, metric1, metric2, metric3, metric4,
	}, `name`)

	assert.Len(merged, 2)
	merge1 := merged[0]
	merge2 := merged[1]

	assert.Equal(`mobius.test.metrics.merge`, merge1.GetName())
	assert.Equal(235, len(merge1.Points()))
	assert.True(sort.IsSorted(merge1.Points()))
	assert.Equal(map[string]interface{}{
		`class`:    []interface{}{`onesies`, `twosies`},
		`cool`:     `beans`,
		`instance`: []interface{}{int64(1), int64(2), int64(3)},
	}, merge1.GetTags())

	assert.Equal(`mobius.test.metrics.othermerge`, merge2.GetName())
	assert.Equal(55, len(merge2.Points()))
	assert.True(sort.IsSorted(merge2.Points()))
	assert.Equal(map[string]interface{}{
		`other`: []interface{}{int64(1), int64(2)},
	}, merge2.GetTags())
}

func TestMetricMergeOnTags(t *testing.T) {
	assert := require.New(t)

	metric0 := NewMetric(`mobius.test.metrics.merge,cool=beans,instance=1,class=onesies`)
	metric1 := NewMetric(`mobius.test.metrics.merge,cool=beans,instance=2,class=onesies`)
	metric2 := NewMetric(`mobius.test.metrics.merge,cool=beans,instance=3,class=twosies`)
	metric3 := NewMetric(`mobius.test.metrics.othermerge,instance=1,class=onesies,other=1`)
	metric4 := NewMetric(`mobius.test.metrics.othermerge,instance=2,class=twosies,other=2`)

	for i := 0; i < 35; i++ {
		metric0.Push(time.Date(2006, 1, 2, 15, 4, i, 0, mst), float64(i))
	}

	for i := 0; i < 100; i++ {
		metric1.Push(time.Date(2006, 1, 2, 15, 4, i, 0, mst), float64(i))
	}

	for i := 0; i < 100; i++ {
		metric2.Push(time.Date(2006, 1, 2, 15, 4, i, rand.Intn(100000), mst), float64(i))
	}

	for i := 0; i < 27; i++ {
		metric3.Push(time.Date(2006, 1, 2, 15, 4, i, rand.Intn(100000), mst), float64(i))
	}

	for i := 0; i < 28; i++ {
		metric4.Push(time.Date(2006, 1, 2, 15, 4, i, rand.Intn(100000), mst), float64(i))
	}

	merged := MergeMetrics([]*Metric{
		metric0, metric1, metric2, metric3, metric4,
	}, `class`)

	assert.Len(merged, 2)
	merge1 := merged[0]
	merge2 := merged[1]

	assert.Equal(`mobius.test.metrics`, merge1.GetName())
	assert.Equal(162, len(merge1.Points()))
	assert.True(sort.IsSorted(merge1.Points()))
	assert.Equal(map[string]interface{}{
		`class`:    `onesies`,
		`cool`:     `beans`,
		`other`:    int64(1),
		`instance`: []interface{}{int64(1), int64(2), int64(1)},
	}, merge1.GetTags())

	assert.Equal(`mobius.test.metrics`, merge2.GetName())
	assert.Equal(128, len(merge2.Points()))
	assert.True(sort.IsSorted(merge2.Points()))
	assert.Equal(map[string]interface{}{
		`class`:    `twosies`,
		`cool`:     `beans`,
		`other`:    int64(2),
		`instance`: []interface{}{int64(3), int64(2)},
	}, merge2.GetTags())
}

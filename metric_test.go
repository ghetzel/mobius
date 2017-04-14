package mobius

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

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
			Reducer: Min,
			Values:  []float64{0, 10, 40, 70},
		}, {
			Reducer: Max,
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

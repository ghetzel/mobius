package mobius

import (
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestPointBucketing(t *testing.T) {
	assert := require.New(t)

	metric := NewMetric(`mobius.test.pointbucket`)

	for i := 0; i < 100; i++ {
		metric.Push(time.Date(2006, 1, 2, 15, 4, i, 0, mst), 0.1+float64(rand.Float64()*float64(i)))
	}

	input := metric.Points()
	assert.Equal(100, len(input))

	output := MakeTimeBuckets(input, 30*time.Second)
	assert.Equal(4, len(output))

	bucket := output[0]
	assert.Equal(10, len(bucket))
	assert.True(bucket[0].Timestamp.Before(bucket[len(bucket)-1].Timestamp))

	bucket = output[1]
	assert.Equal(30, len(bucket))
	assert.True(bucket[0].Timestamp.Before(bucket[len(bucket)-1].Timestamp))

	bucket = output[2]
	assert.Equal(30, len(bucket))
	assert.True(bucket[0].Timestamp.Before(bucket[len(bucket)-1].Timestamp))

	bucket = output[3]
	assert.Equal(30, len(bucket))
	assert.True(bucket[0].Timestamp.Before(bucket[len(bucket)-1].Timestamp))
}

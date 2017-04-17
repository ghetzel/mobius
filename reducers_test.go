package mobius

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReduceGetName(t *testing.T) {
	assert := require.New(t)

	assert.Equal(`sum`, GetReducerName(`sum`))
	assert.Equal(`inter-quartile-range`, GetReducerName(`inter-quartile-range`))
	assert.Equal(`inter-quartile-range`, GetReducerName(`iqr`))
	assert.Equal(`maximum`, GetReducerName(`maximum`))
	assert.Equal(`maximum`, GetReducerName(`max`))
}

func TestReduceFirst(t *testing.T) {
	assert := require.New(t)

	assert.Equal(float64(0), Reduce(First))
	assert.Equal(float64(0), Reduce(First, 0))
	assert.Equal(float64(1), Reduce(First, 1, -1))
	assert.Equal(float64(-1), Reduce(First, -1, 1))
	assert.Equal(float64(1), Reduce(First, 1))
	assert.Equal(float64(1.1), Reduce(First, 1.1, 2.2, 3.3))
}

func TestReduceLast(t *testing.T) {
	assert := require.New(t)

	assert.Equal(float64(0), Reduce(Last))
	assert.Equal(float64(0), Reduce(Last, 0))
	assert.Equal(float64(-1), Reduce(Last, 1, -1))
	assert.Equal(float64(1), Reduce(Last, -1, 1))
	assert.Equal(float64(1), Reduce(Last, 1))
	assert.Equal(float64(3.3), Reduce(Last, 1.1, 2.2, 3.3))
}

func TestReduceCount(t *testing.T) {
	assert := require.New(t)

	assert.Equal(float64(0), Reduce(Count))
	assert.Equal(float64(1), Reduce(Count, 0))
	assert.Equal(float64(2), Reduce(Count, 1, -1))
	assert.Equal(float64(2), Reduce(Count, -1, 1))
	assert.Equal(float64(1), Reduce(Count, 1))
	assert.Equal(float64(3), Reduce(Count, 1.1, 2.2, 3.3))
}

func TestReduceSum(t *testing.T) {
	assert := require.New(t)

	assert.Equal(float64(0), Reduce(Sum))
	assert.Equal(float64(0), Reduce(Sum, 0))
	assert.Equal(float64(0), Reduce(Sum, 1, -1))
	assert.Equal(float64(1), Reduce(Sum, 1))
	assert.Equal(float64(6.6), Reduce(Sum, 1.1, 2.2, 3.3))
}

func TestReduceMin(t *testing.T) {
	assert := require.New(t)

	assert.Equal(float64(0), Reduce(Minimum))
	assert.Equal(float64(0), Reduce(Minimum, 0))
	assert.Equal(float64(1), Reduce(Minimum, 1))
	assert.Equal(float64(-1), Reduce(Minimum, 1, -1))
	assert.Equal(float64(1.1), Reduce(Minimum, 1.1, 2.2, 3.3))
}

func TestReduceStdDev(t *testing.T) {
	assert := require.New(t)

	assert.Equal(float64(0), Reduce(StdDev))
	assert.Equal(float64(0), Reduce(StdDev, 0))
	assert.Equal(float64(0), Reduce(StdDev, 1))
	assert.Equal(float64(2), Reduce(StdDev, 2, 4, 4, 4, 5, 5, 7, 9))
}

func TestReduceVariance(t *testing.T) {
	assert := require.New(t)

	assert.Equal(float64(0), Reduce(Variance))
	assert.Equal(float64(0), Reduce(Variance, 0))
	assert.Equal(float64(0), Reduce(Variance, 1))
	assert.Equal(float64(4), Reduce(Variance, 2, 4, 4, 4, 5, 5, 7, 9))
}

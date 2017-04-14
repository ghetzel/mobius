package mobius

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

var mst = time.FixedZone(`MST`, -7*3600)

func TestDatasetCRUD(t *testing.T) {
	assert := require.New(t)

	tempPath, err := ioutil.TempDir(``, `mobius_test_`)
	defer os.RemoveAll(tempPath)

	assert.NoError(err)

	database, err := OpenDataset(tempPath)
	assert.NoError(err)
	assert.NotNil(database)

	metric := NewMetric(`mobius.test.event1,test=one,crud=yes,age=2,factor=3.14`)

	assert.Equal(map[string]interface{}{
		`crud`:   true,
		`test`:   `one`,
		`age`:    int64(2),
		`factor`: 3.14,
	}, metric.GetTags())

	metrics := make([]*Metric, 0)

	for i := 0; i < 10; i++ {
		metric.Push(time.Date(2006, 1, 2, 15, 4, 5+i, 0, mst), float64(1.2*float64(i+1)))

		metrics = append(metrics, metric)
	}

	for _, metric := range metrics {
		assert.NoError(database.Write(metric))
	}

	metrics, err = database.Range(time.Time{}, time.Now(), `mobius.test.event1`)
	assert.NoError(err)

	assert.NotEmpty(metrics)
	assert.Equal(10, len(metrics[0].Points()))

	// remove points before (exclusive) a given date
	n, err := database.TrimBefore(time.Date(2006, 1, 2, 15, 4, 5+2, 0, mst))
	assert.NoError(err)
	assert.Equal(int64(2), n)

	// verify expected length
	metrics, err = database.Range(time.Time{}, time.Now(), `mobius.test.event1`)
	assert.NoError(err)
	assert.Equal(8, len(metrics[0].Points()))

	// remove points after (inclusive) a given date
	n, err = database.TrimAfter(time.Date(2006, 1, 2, 15, 4, 5+6, 0, mst))
	assert.NoError(err)
	assert.Equal(int64(4), n)

	// verify expected length
	metrics, err = database.Range(time.Time{}, time.Now(), `mobius.test.event1`)
	assert.NoError(err)
	assert.Equal(4, len(metrics[0].Points()))

	// remove whole series
	n, err = database.Remove(`mobius.test.event1`)
	assert.NoError(err)
	assert.Equal(int64(1), n)

	// verify expected length
	metrics, err = database.Range(time.Time{}, time.Now(), `mobius.test.event1`)
	assert.NoError(err)
	assert.Empty(metrics)

	names, err := database.GetNames(`**`)
	assert.NoError(err)
	assert.Empty(names)
}

func TestDatasetKeyGlobbing(t *testing.T) {
	assert := require.New(t)

	tempPath, err := ioutil.TempDir(``, `mobius_test_`)
	defer os.RemoveAll(tempPath)

	assert.NoError(err)

	database, err := OpenDataset(tempPath)
	assert.NoError(err)
	assert.NotNil(database)

	metrics := make([]*Metric, 0)

	for i := 0; i < 100; i++ {
		metric := NewMetric(fmt.Sprintf("mobius.test%02d.keytest%04d,test=true,instance=%d", (i % 10), i, int(i%7)))

		metric.Push(time.Date(2006, 1, 2, 15, 4, 5+i, 0, mst), float64(1.2*float64(i+1)))

		metrics = append(metrics, metric)
	}

	for _, metric := range metrics {
		database.Write(metric)
	}

	names, err := database.GetNames(`**`)
	assert.NoError(err)
	assert.Equal(100, len(names))

	names, err = database.GetNames(`**.test02.*`)
	assert.NoError(err)
	assert.Equal(10, len(names))

	names, err = database.GetNames(`**.test0{1,3,5,7,9}.*`)
	assert.NoError(err)
	assert.Equal(50, len(names))

	names, err = database.GetNames(`**.test0{1,3,5,7,9}.**,instance={1,3,5},?**`)
	assert.NoError(err)
	assert.Equal(22, len(names))

	names, err = database.GetNames(`**,instance=4,**test=true`)
	assert.NoError(err)
	assert.Equal(14, len(names))
}

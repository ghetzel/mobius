package mobius

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"
)

var mst = time.FixedZone(`MST`, -7*3600)

func TestDatasetCRUD(t *testing.T) {
	assert := require.New(t)

	tempPath, err := ioutil.TempDir(``, `mobius_test_`)
	defer os.RemoveAll(tempPath)

	assert.NoError(err)

	database, err := OpenDataset(path.Join(tempPath, `test.db`))
	assert.NoError(err)
	assert.NotNil(database)

	metric := NewMetric(`mobius.test.event1`)

	metrics := make([]*Metric, 0)

	for i := 0; i < 10; i++ {
		metric.Push(&Point{
			Timestamp: time.Date(2006, 1, 2, 15, 4, 5+i, 0, mst),
			Value:     float64(1.2 * float64(i+1)),
		})

		metrics = append(metrics, metric)
	}

	for _, metric := range metrics {
		for _, point := range metric.Points {
			assert.NoError(database.Write(metric, point))
		}
	}

	metrics, err = database.Range(time.Time{}, time.Now(), `mobius.test.event1`)
	assert.NoError(err)

	assert.NotEmpty(metrics)
	assert.Equal(10, len(metrics[0].Points))
}

func TestDatasetKeyGlobbing(t *testing.T) {
	assert := require.New(t)

	tempPath, err := ioutil.TempDir(``, `mobius_test_`)
	defer os.RemoveAll(tempPath)

	assert.NoError(err)

	database, err := OpenDataset(path.Join(tempPath, `test.db`))
	assert.NoError(err)
	assert.NotNil(database)

	metrics := make([]*Metric, 0)

	for i := 0; i < 100; i++ {
		metric := NewMetric(fmt.Sprintf("mobius.test%02d.keytest%04d", (i % 10), i))

		metric.Push(&Point{
			Timestamp: time.Date(2006, 1, 2, 15, 4, 5+i, 0, mst),
			Value:     float64(1.2 * float64(i+1)),
		})

		metrics = append(metrics, metric)
	}

	for _, metric := range metrics {
		for _, point := range metric.Points {
			assert.NoError(database.Write(metric, point))
		}
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
}

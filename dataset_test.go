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

func TestDatabaseCRUD(t *testing.T) {
	assert := require.New(t)

	tempPath, err := ioutil.TempDir(``, `fennecdb_test_`)
	defer os.RemoveAll(tempPath)

	assert.NoError(err)

	database, err := OpenDatabase(tempPath)
	assert.NoError(err)
	assert.NotNil(database)

	event1 := NewMetric(`fennecdb.test.event1`, -1, nil)

	events := []MetricEvent{}

	for i := 0; i < 10; i++ {
		events = append(events, MetricEvent{
			Metric: event1,
			Point: &Point{
				Timestamp: Timestamp{
					Time: time.Date(2006, 1, 2, 15, 4, 5+i, 0, mst),
				},
				Value: float64(1.2 * float64(i)),
			},
		})
	}

	for _, event := range events {
		assert.NoError(database.Write(event))
	}

	metrics, err := database.Range(time.Time{}, time.Now(), `fennecdb.test.event1`)
	assert.NoError(err)

	assert.NotEmpty(metrics)
	points, ok := metrics[`fennecdb.test.event1`]
	assert.True(ok)
	assert.Equal(9, len(points))
}

func TestDatabaseKeyGlobbing(t *testing.T) {
	assert := require.New(t)

	tempPath, err := ioutil.TempDir(``, `fennecdb_test_`)
	defer os.RemoveAll(tempPath)

	assert.NoError(err)

	database, err := OpenDatabase(tempPath)
	assert.NoError(err)
	assert.NotNil(database)

	events := []MetricEvent{}

	for i := 0; i <= 100; i++ {
		event1 := NewMetric(fmt.Sprintf("fennecdb.test%02d.keytest%04d", (i%10), i), -1, nil)

		events = append(events, MetricEvent{
			Metric: event1,
			Point: &Point{
				Timestamp: Timestamp{
					Time: time.Date(2006, 1, 2, 15, 4, 5+i, 0, mst),
				},
				Value: float64(1.2 * float64(i)),
			},
		})
	}

	for _, event := range events {
		assert.NoError(database.Write(event))
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

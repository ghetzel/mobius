package mobius

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func jsonbody(r io.Reader, t interface{}) interface{} {
	if data, err := ioutil.ReadAll(r); err == nil {
		if err := json.Unmarshal(data, &t); err == nil {
			return t
		}
	}

	return nil
}

func TestServer(t *testing.T) {
	var body interface{}

	assert := require.New(t)
	tempPath, err := ioutil.TempDir(``, `mobius_test_`)
	defer os.RemoveAll(tempPath)
	assert.NoError(err)

	database, err := OpenDataset(tempPath)
	assert.NoError(err)
	assert.NotNil(database)

	for i := 0; i < 10; i++ {
		metric := NewMetric(fmt.Sprintf("mobius.test.servertest.event%d,test=one,crud=yes,age=2,factor=3.14", i))
		metric.Push(time.Date(2006, 1, 2, 15, 4, 5+i, 0, mst), float64(1.2*float64(i+1)))
		assert.NoError(database.Write(metric))
	}

	server := NewServer(database)

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(`GET`, `/metrics/list`, nil))
	response := recorder.Result()
	assert.Equal(200, response.StatusCode)

	body = make([]string, 0)
	assert.Equal([]interface{}{
		map[string]interface{}{
			`name`: `mobius.test.servertest.event0`,
			`tags`: map[string]interface{}{
				`age`:    float64(2),
				`crud`:   true,
				`factor`: 3.14,
				`test`:   `one`,
			},
			`unique_name`: `mobius.test.servertest.event0,age=2,crud=true,factor=3.14,test=one`,
		},
		map[string]interface{}{
			`name`: `mobius.test.servertest.event1`,
			`tags`: map[string]interface{}{
				`age`:    float64(2),
				`crud`:   true,
				`factor`: 3.14,
				`test`:   `one`,
			},
			`unique_name`: `mobius.test.servertest.event1,age=2,crud=true,factor=3.14,test=one`,
		},
		map[string]interface{}{
			`name`: `mobius.test.servertest.event2`,
			`tags`: map[string]interface{}{
				`age`:    float64(2),
				`crud`:   true,
				`factor`: 3.14,
				`test`:   `one`,
			},
			`unique_name`: `mobius.test.servertest.event2,age=2,crud=true,factor=3.14,test=one`,
		},
		map[string]interface{}{
			`name`: `mobius.test.servertest.event3`,
			`tags`: map[string]interface{}{
				`age`:    float64(2),
				`crud`:   true,
				`factor`: 3.14,
				`test`:   `one`,
			},
			`unique_name`: `mobius.test.servertest.event3,age=2,crud=true,factor=3.14,test=one`,
		},
		map[string]interface{}{
			`name`: `mobius.test.servertest.event4`,
			`tags`: map[string]interface{}{
				`age`:    float64(2),
				`crud`:   true,
				`factor`: 3.14,
				`test`:   `one`,
			},
			`unique_name`: `mobius.test.servertest.event4,age=2,crud=true,factor=3.14,test=one`,
		},
		map[string]interface{}{
			`name`: `mobius.test.servertest.event5`,
			`tags`: map[string]interface{}{
				`age`:    float64(2),
				`crud`:   true,
				`factor`: 3.14,
				`test`:   `one`,
			},
			`unique_name`: `mobius.test.servertest.event5,age=2,crud=true,factor=3.14,test=one`,
		},
		map[string]interface{}{
			`name`: `mobius.test.servertest.event6`,
			`tags`: map[string]interface{}{
				`age`:    float64(2),
				`crud`:   true,
				`factor`: 3.14,
				`test`:   `one`,
			},
			`unique_name`: `mobius.test.servertest.event6,age=2,crud=true,factor=3.14,test=one`,
		},
		map[string]interface{}{
			`name`: `mobius.test.servertest.event7`,
			`tags`: map[string]interface{}{
				`age`:    float64(2),
				`crud`:   true,
				`factor`: 3.14,
				`test`:   `one`,
			},
			`unique_name`: `mobius.test.servertest.event7,age=2,crud=true,factor=3.14,test=one`,
		},
		map[string]interface{}{
			`name`: `mobius.test.servertest.event8`,
			`tags`: map[string]interface{}{
				`age`:    float64(2),
				`crud`:   true,
				`factor`: 3.14,
				`test`:   `one`,
			},
			`unique_name`: `mobius.test.servertest.event8,age=2,crud=true,factor=3.14,test=one`,
		},
		map[string]interface{}{
			`name`: `mobius.test.servertest.event9`,
			`tags`: map[string]interface{}{
				`age`:    float64(2),
				`crud`:   true,
				`factor`: 3.14,
				`test`:   `one`,
			},
			`unique_name`: `mobius.test.servertest.event9,age=2,crud=true,factor=3.14,test=one`,
		},
	}, jsonbody(response.Body, body))
}

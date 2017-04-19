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
		metric := NewMetric(fmt.Sprintf("mobius.test.servertest.event%d:test=one,crud=yes,age=2,factor=3.14", i))
		metric.Push(time.Date(2006, 1, 2, 15, 4, 5+i, 0, mst), float64(1.2*float64(i+1)))
		assert.NoError(database.Write(metric))
	}

	server := NewServer(database)

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(`GET`, `/metrics/list`, nil))
	response := recorder.Result()
	assert.Equal(200, response.StatusCode)

	body = make([]string, 0)
	out := make([]interface{}, 10)

	for i := 0; i < 10; i++ {
		out[i] = map[string]interface{}{
			`name`: fmt.Sprintf("mobius.test.servertest.event%d", i),
			`tags`: map[string]interface{}{
				`age`:    float64(2),
				`crud`:   true,
				`factor`: 3.14,
				`test`:   `one`,
			},
			`unique_name`: fmt.Sprintf("mobius.test.servertest.event%d:age=2,crud=true,factor=3.14,test=one", i),
		}
	}

	assert.Equal(out, jsonbody(response.Body, body))
}

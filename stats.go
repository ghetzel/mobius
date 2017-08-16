package mobius

import (
	"net/http"
	"os"
	"time"

	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/pathutil"
)

var Database *Dataset
var StatsPrefix string
var basetags = make(map[string]interface{})

func Initialize(statsdir string, tags map[string]interface{}) error {
	if len(tags) > 0 {
		basetags = tags
	}

	if expandedStatsDir, err := pathutil.ExpandUser(statsdir); err == nil {
		// autocreate parent directory
		if _, err := os.Stat(expandedStatsDir); os.IsNotExist(err) {
			if err := os.MkdirAll(expandedStatsDir, 0755); err != nil {
				return err
			}
		}

		if dataset, err := OpenDataset(expandedStatsDir); err == nil {
			Database = dataset
		} else {
			return err
		}
	} else {
		return err
	}

	return nil
}

func CreateServer(urlPrefix string) http.Handler {
	return http.StripPrefix(urlPrefix, NewServer(Database))
}

func IsEnabled() bool {
	if Database == nil {
		return false
	}

	return true
}

func Cleanup() {
	if Database != nil {
		Database.Close()
		Database = nil
	}
}

func Increment(name string, tags ...map[string]interface{}) {
	IncrementN(name, 1, tags...)
}

func IncrementN(name string, count int, tags ...map[string]interface{}) {

	if Database != nil {
		m := metric(name, tags)
		Database.Write(m.Push(time.Now(), float64(count)))
	}
}

func Gauge(name string, value float64, tags ...map[string]interface{}) {
	if Database != nil {
		m := metric(name, tags)
		Database.Write(m.Push(time.Now(), value))
	}
}

func Set(name string, value float64, tags ...map[string]interface{}) {
	if Database != nil {
		m := metric(name, tags)
		m.MaxSize = 1
		Database.Write(m.Push(time.Now(), value))
	}
}

func metric(name string, tags []map[string]interface{}) *Metric {
	outTags := basetags

	if len(tags) > 0 {
		if v, err := maputil.Merge(basetags, tags[0]); err == nil {
			outTags = v
		} else {
			panic("invalid map merge: " + err.Error())
		}
	}

	if len(outTags) > 0 {
		name = name + NameTagsDelimiter + maputil.Join(outTags, `=`, InlineTagSeparator)
	}

	name = StatsPrefix + name

	return NewMetric(name)
}

package mobius

import (
	"encoding/json"
	"fmt"
	"github.com/ghetzel/go-stockutil/httputil"
	"github.com/ghetzel/go-stockutil/maputil"
	"github.com/ghetzel/go-stockutil/sliceutil"
	"github.com/ghetzel/go-stockutil/typeutil"
	"github.com/husobee/vestigo"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

var DefaultMetricReducerFunc = `sum`

type Server struct {
	router  *vestigo.Router
	dataset *Dataset
	prefix  string
}

type metricSummary struct {
	Name       string                 `json:"name"`
	Tags       map[string]interface{} `json:"tags,omitempty"`
	Statistics map[string]float64     `json:"statistics"`
}

func NewServer(dataset *Dataset) *Server {
	router := vestigo.NewRouter()

	router.Get(`/metrics/list`, func(w http.ResponseWriter, req *http.Request) {
		if names, err := dataset.GetNames(httputil.Q(req, `filter`, `**`)); err == nil {
			metrics := make([]*Metric, len(names))

			for i, name := range names {
				metrics[i] = NewMetric(name)
			}

			respond(w, metrics)
		} else {
			respond(w, err)
		}
	})

	router.Get(`/metrics/:action/*`, func(w http.ResponseWriter, req *http.Request) {
		action := vestigo.Param(req, `action`)
		nameset := strings.Split(vestigo.Param(req, `_name`), `;`)
		var start, end time.Time
		var aggregateInterval time.Duration

		groupByField := httputil.Q(req, `group`, `name`)

		if v, err := ParseTimeString(httputil.Q(req, `from`, `-1h`)); err == nil {
			start = v
		} else {
			respond(w, err, http.StatusBadRequest)
			return
		}

		if v, err := ParseTimeString(httputil.Q(req, `to`)); err == nil {
			end = v
		} else {
			respond(w, err, http.StatusBadRequest)
			return
		}

		if v := httputil.Q(req, `interval`, `1s`); v != `none` {
			if d, err := time.ParseDuration(v); err == nil {
				aggregateInterval = d
			} else {
				respond(w, err, http.StatusBadRequest)
				return
			}
		}

		if metrics, err := dataset.Range(start, end, nameset...); err == nil {
			// regroup the metrics according to the given field
			metrics = MergeMetrics(metrics, groupByField)

			switch action {
			case `query`:
				// if we're consolidating metrics into time buckets, do so now
				if aggregateInterval > 0 {
					gfn := httputil.Q(req, `fn`, DefaultMetricReducerFunc)
					if reducer, ok := GetReducer(gfn); ok {
						for i, metric := range metrics {
							metrics[i] = metric.Consolidate(aggregateInterval, reducer)
						}
					} else {
						respond(w, fmt.Errorf("Unknown grouping function '%s'", gfn), http.StatusBadRequest)
						return
					}
				}

				switch httputil.Q(req, `format`) {
				case `csv`:
					w.Header().Set(`Content-Type`, `text/plain`)
					writeTsv(w, metrics)
				default:
					respond(w, metrics)
				}

			case `summary`:
				gfn := strings.Split(httputil.Q(req, `fn`, DefaultMetricReducerFunc), `,`)
				reducers := make([]ReducerFunc, len(gfn))

				for i, name := range gfn {
					if r, ok := GetReducer(name); ok {
						reducers[i] = r
					} else {
						respond(w, fmt.Errorf("Unknown grouping function '%s'", name), http.StatusBadRequest)
						return
					}
				}

				summary := make([]metricSummary, 0)

				for _, metric := range metrics {
					metricStats := make(map[string]float64)

					for i, value := range SummarizeMetric(metric, reducers...) {
						key := strings.Replace(GetReducerName(gfn[i]), `-`, `_`, -1)
						metricStats[key] = value
					}

					summary = append(summary, metricSummary{
						Name:       metric.GetName(),
						Tags:       metric.GetTags(),
						Statistics: metricStats,
					})
				}

				respond(w, summary)
			default:
				respond(w, `Not Found`, http.StatusNotFound)
			}
		} else {
			respond(w, err)
		}
	})

	return &Server{
		router:  router,
		dataset: dataset,
	}
}

func (self *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	self.router.ServeHTTP(w, req)
}

func writeTsv(w io.Writer, metrics []*Metric) {
	tags := make([]string, 0)

	for _, metric := range metrics {
		for _, tag := range maputil.StringKeys(metric.GetTags()) {
			if !sliceutil.ContainsString(tags, tag) {
				tags = append(tags, tag)
			}
		}
	}

	sort.Strings(tags)

	tagset := strings.Join(tags, "\t")

	if tagset != `` {
		tagset = "\t" + tagset
	}

	fmt.Fprintf(w, "name\ttime\tvalue%s\n", tagset)

	for _, metric := range metrics {
		line := make([]string, len(tags)+3)

		line[0] = metric.GetName()

		for i, tag := range tags {
			if v := metric.GetTag(tag); v != nil {
				if typeutil.IsArray(v) {
					line[3+i] = fmt.Sprintf("%v", strings.Join(sliceutil.Stringify(v), `,`))
				} else {
					line[3+i] = fmt.Sprintf("%v", v)
				}
			}
		}

		for _, point := range metric.Points() {
			actualLine := make([]string, len(tags)+3)
			copy(actualLine, line)

			actualLine[1] = fmt.Sprintf("%d", point.Timestamp.UnixNano()/int64(time.Millisecond))
			actualLine[2] = fmt.Sprintf("%f", point.Value)

			fmt.Fprintf(w, "%v\n", strings.Join(actualLine, "\t"))
		}
	}
}

func respond(w http.ResponseWriter, data interface{}, code ...int) {
	w.Header().Set(`Content-Type`, `application/json`)

	if err, ok := data.(error); ok {
		data = map[string]interface{}{
			`error`: err.Error(),
		}

		if len(code) == 0 || code[0] < 400 {
			code = []int{http.StatusInternalServerError}
		}
	}

	if output, err := json.MarshalIndent(data, ``, `  `); err == nil {
		if len(code) > 0 {
			w.WriteHeader(code[0])
		}

		w.Write(output)
	} else {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

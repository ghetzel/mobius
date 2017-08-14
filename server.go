package mobius

import (
	"encoding/json"
	"fmt"
	"github.com/ghetzel/go-stockutil/httputil"
	"github.com/husobee/vestigo"
	"net/http"
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
			palette := getPalette(req)

			for i, name := range names {
				metrics[i] = NewMetric(name)

				if palette != nil {
					metrics[i].Metadata[`color`] = palette.Get(i)
				}
			}

			respond(w, metrics)
		} else {
			respond(w, err)
		}
	})

	router.Get(`/metrics/:action/*`, func(w http.ResponseWriter, req *http.Request) {
		action := vestigo.Param(req, `action`)
		nameset := strings.Split(vestigo.Param(req, `_name`), `;`)
		palette := getPalette(req)

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
			format := httputil.Q(req, `format`)

			switch action {
			case `query`:
				// if we're consolidating metrics into time buckets, do so now
				if aggregateInterval > 0 {
					gfn := httputil.Q(req, `fn`, DefaultMetricReducerFunc)
					if reducer, ok := GetReducer(gfn); ok {
						for i, metric := range metrics {
							metrics[i] = metric.Consolidate(aggregateInterval, reducer)

							if palette != nil {
								metrics[i].Metadata[`color`] = palette.Get(i)
							}
						}
					} else {
						respond(w, fmt.Errorf("Unknown grouping function '%s'", gfn), http.StatusBadRequest)
						return
					}
				}

				switch format {
				case `png`, `svg`:
					graph := NewGraph(metrics)

					graph.Options.Title = httputil.Q(req, `title`)
					graph.Options.Width = int(httputil.QInt(req, `width`))
					graph.Options.Height = int(httputil.QInt(req, `height`))
					graph.Options.DPI = httputil.QFloat(req, `dpi`, 72)

					switch format {
					case `png`:
						w.Header().Set(`Content-Type`, `image/png`)
					case `svg`:
						w.Header().Set(`Content-Type`, `image/svg+xml`)
					}

					if err := graph.Render(w, RenderFormat(format)); err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
					}
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

func getPalette(req *http.Request) Palette {
	if v := httputil.Q(req, `palette`); v != `` {
		switch v {
		case `spectrum14`:
			return PaletteSpectrum14
		case `spectrum2000`:
			return PaletteSpectrum2000
		case `classic9`:
			return PaletteClassic9
		case `munin`:
			return PaletteMunin
		default:
			return Palette(strings.Split(v, `,`))
		}
	}

	return nil
}

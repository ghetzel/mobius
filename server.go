package mobius

import (
	"encoding/json"
	"github.com/ghetzel/go-stockutil/httputil"
	"github.com/husobee/vestigo"
	"net/http"
	"strings"
	"time"
)

type Server struct {
	router  *vestigo.Router
	dataset *Dataset
	prefix  string
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

	router.Get(`/metrics/query/*`, func(w http.ResponseWriter, req *http.Request) {
		nameset := strings.Split(vestigo.Param(req, `_name`), `;`)
		var start, end time.Time

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

		if metrics, err := dataset.Range(start, end, nameset...); err == nil {
			respond(w, metrics)
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

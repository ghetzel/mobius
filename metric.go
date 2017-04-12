package mobius

import (
	"strings"
	"time"
)

var DefaultTagSeparator = ` `

type Point struct {
	Timestamp time.Time `json:"time"`
	Value     float64   `json:"value"`
}

type Metric struct {
	Name   string                 `json:"name"`
	Tags   map[string]interface{} `json:"tags,omitempty"`
	Points []*Point               `json:"points"`
}

func NewMetric(name string) *Metric {
	name, properties := SplitNameProperties(name, DefaultTagSeparator)

	return &Metric{
		Name:   name,
		Tags:   properties,
		Points: make([]*Point, 0),
	}
}

func (self *Metric) Push(point *Point) {
	self.Points = append(self.Points, point)
}

func SplitNameProperties(name string, sep string) (string, map[string]interface{}) {
	properties := make(map[string]interface{})
	parts := strings.Split(name, sep)

	if len(parts) > 1 {
		for _, pair := range parts[1:] {
			kv := strings.SplitN(pair, `=`, 2)

			if len(kv) == 2 {
				properties[kv[0]] = kv[1]
			}
		}

		name = parts[0]
	}

	return name, properties
}

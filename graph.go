package mobius

import (
	"fmt"
	"github.com/wcharczuk/go-chart"
	"io"
	"time"
)

var DefaultDPI float64 = 72.0

type RenderFormat string

const (
	RenderFormatPNG RenderFormat = `png`
	RenderFormatSVG              = `svg`
)

type GraphOptions struct {
	Title  string  `json:"title"`
	Width  int     `json:"width"`
	Height int     `json:"height"`
	DPI    float64 `json:"dpi"`
}

type Graph struct {
	Series  []*Metric
	Options GraphOptions
	Style   GraphStyle
}

func NewGraph(metrics []*Metric) *Graph {
	return &Graph{
		Series:  metrics,
		Options: GraphOptions{},
		Style:   DefaultStyle,
	}
}

func (self *Graph) Render(w io.Writer, format RenderFormat) error {
	graph := chart.Chart{
		Title:      self.Options.Title,
		TitleStyle: self.Style.Title,
		Background: self.Style.Background,
		Canvas:     self.Style.Canvas,
		Series:     make([]chart.Series, 0),
		XAxis: chart.XAxis{
			Style:          self.Style.XAxis,
			NameStyle:      self.Style.XAxisTitle,
			TickStyle:      self.Style.XAxisTicks,
			GridMajorStyle: self.Style.XAxisGridMajor,
			GridMinorStyle: self.Style.XAxisGridMinor,
		},
		YAxis: chart.YAxis{
			Style:          self.Style.YAxis,
			NameStyle:      self.Style.YAxisTitle,
			TickStyle:      self.Style.YAxisTicks,
			GridMajorStyle: self.Style.YAxisGridMajor,
			GridMinorStyle: self.Style.YAxisGridMinor,
		},
		YAxisSecondary: chart.YAxis{
			Style:          self.Style.YAxis2,
			NameStyle:      self.Style.YAxis2Title,
			TickStyle:      self.Style.YAxis2Ticks,
			GridMajorStyle: self.Style.YAxis2GridMajor,
			GridMinorStyle: self.Style.YAxis2GridMinor,
		},
	}

	if v := self.Options.Width; v > 0 {
		graph.Width = v
	}

	if v := self.Options.Height; v > 0 {
		graph.Height = v
	}

	if v := self.Options.DPI; v > 0 {
		graph.DPI = v
	} else {
		graph.DPI = DefaultDPI
	}

	for i, metric := range self.Series {
		series := chart.TimeSeries{
			Name:    metric.GetUniqueName(),
			Style:   self.Style.GetSeriesStyle(i),
			XValues: make([]time.Time, 0),
			YValues: make([]float64, 0),
		}

		if points := metric.Points(); len(points) > 0 {
			series.XValues = points.Timestamps()
			series.YValues = points.Values()
		}

		graph.Series = append([]chart.Series{series}, graph.Series...)
	}

	var renderProvider chart.RendererProvider

	switch format {
	case RenderFormatPNG:
		renderProvider = chart.PNG
	case RenderFormatSVG:
		renderProvider = chart.SVG
	default:
		return fmt.Errorf("Unsupported format %q", format)
	}

	if err := graph.Render(renderProvider, w); err != nil {
		return err
	}

	return nil
}

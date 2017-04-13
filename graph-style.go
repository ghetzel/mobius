package mobius

import (
	"github.com/wcharczuk/go-chart"
)

type GraphStyle struct {
	Title           chart.Style
	Background      chart.Style
	Canvas          chart.Style
	XAxis           chart.Style
	XAxisTitle      chart.Style
	XAxisTicks      chart.Style
	XAxisGridMajor  chart.Style
	XAxisGridMinor  chart.Style
	YAxis           chart.Style
	YAxisTitle      chart.Style
	YAxisTicks      chart.Style
	YAxisGridMajor  chart.Style
	YAxisGridMinor  chart.Style
	YAxis2          chart.Style
	YAxis2Title     chart.Style
	YAxis2Ticks     chart.Style
	YAxis2GridMajor chart.Style
	YAxis2GridMinor chart.Style
	Series          []chart.Style
}

func (self *GraphStyle) GetSeriesStyle(i int) chart.Style {
	if len(self.Series) == 0 {
		return chart.Style{}
	} else {
		return self.Series[i%len(self.Series)]
	}
}

var DefaultStyle = GraphStyle{
	XAxis: chart.Style{
		Show: true,
	},
	YAxis: chart.Style{
		Show: true,
	},
	YAxis2: chart.Style{
		Show: false,
	},
	Series: MakeSimplePalette(func(style *chart.Style) {
		style.StrokeWidth = 2
	}, PaletteSpectrum2000...),
}

package mobius

import (
	"github.com/wcharczuk/go-chart"
	"github.com/wcharczuk/go-chart/drawing"
	"strings"
)

type Palette []string

func (self Palette) Get(index int) string {
	if len(self) == 0 {
		return ``
	}

	return `#` + strings.TrimPrefix(self[index%len(self)], `#`)
}

func MakeSimplePalette(each func(style *chart.Style), colors ...string) []chart.Style {
	styles := make([]chart.Style, len(colors))

	for i, color := range colors {
		style := styles[i]

		style.Show = true
		style.StrokeColor = drawing.ColorFromHex(color)
		style.FillColor = drawing.ColorFromHex(color).WithAlpha(64)

		if each != nil {
			each(&style)
		}

		styles[i] = style
	}

	return styles
}

var PaletteSpectrum14 = Palette{
	`387aa3`, `649eb9`, `9dc2d3`, `a888c2`, `d8aad6`,
	`e7cbe6`, `a1d05d`, `bbe468`, `d2ed82`, `716c49`,
	`92875a`, `b2a470`, `dc8f70`, `ecb796`,
}

var PaletteSpectrum2000 = Palette{
	`57306f`, `514c76`, `646583`, `738394`, `6b9c7d`,
	`84b665`, `a7ca50`, `bfe746`, `e2f528`, `fff726`,
	`ecdd00`, `d4b11d`, `de8800`, `de4800`, `c91515`,
	`9a0000`, `7b0429`, `580839`, `31082b`,
}

var PaletteClassic9 = Palette{
	`2f254a`, `491d37`, `7c2626`, `963b20`, `7d5836`,
	`c5a32f`, `ddcb53`, `a2b73c`, `848f39`, `4a6860`,
	`423d4f`,
}

var PaletteMunin = Palette{
	`00cc00`, `0066b3`, `ff8000`, `ffcc00`, `330099`,
	`990099`, `ccff00`, `ff0000`, `808080`, `008f00`,
	`00487d`, `b35a00`, `b38f00`, `6b006b`, `8fb300`,
	`b30000`, `bebebe`, `80ff80`, `80c9ff`, `ffc080`,
	`ffe680`, `aa80ff`, `ee00cc`, `ff8080`, `666600`,
	`ffbfff`, `00ffcc`, `cc6699`, `999900`,
}

package mobius

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"strconv"
)

type KairosFormatter struct {
	Formatter
}

func (self KairosFormatter) Format(metric *Metric, point *Point) string {
	if len(metric.Name) > 0 {
		value := strconv.FormatFloat(point.Value, 'f', -1, 64)
		tags := ``

		if len(metric.Tags) > 0 {
			tags = ` ` + maputil.Join(metric.Tags, `=`, ` `)
		}

		return fmt.Sprintf("put %s %s %s%s",
			metric.Name,
			point.Timestamp.String(),
			value,
			tags)
	} else {
		return ``
	}
}

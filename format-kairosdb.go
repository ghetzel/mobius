package mobius

import (
	"fmt"
	"github.com/ghetzel/go-stockutil/maputil"
	"strconv"
)

type KairosFormatter struct {
	Formatter
}

func (self KairosFormatter) Format(metric IMetric, point *Point) string {
	if len(metric.GetUniqueName()) > 0 {
		value := strconv.FormatFloat(point.Value, 'f', -1, 64)
		tags := ``

		if len(metric.GetTags()) > 0 {
			tags = ` ` + maputil.Join(metric.GetTags(), `=`, ` `)
		}

		return fmt.Sprintf("put %s %s %s%s",
			metric.GetUniqueName(),
			point.Timestamp.String(),
			value,
			tags)
	} else {
		return ``
	}
}

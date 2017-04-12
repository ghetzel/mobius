package mobius

import (
	"fmt"
	"strconv"
)

type CarbonFormatter struct {
	Formatter
}

func (self CarbonFormatter) Format(metric *Metric, point *Point) string {
	if len(metric.Name) > 0 {
		value := strconv.FormatFloat(point.Value, 'f', -1, 64)

		return fmt.Sprintf("%s %s %d",
			metric.Name,
			value,
			point.Timestamp.Unix(),
		)
	} else {
		return ``
	}
}

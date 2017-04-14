package mobius

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type CarbonParser struct {
	Parser
}

func (self CarbonParser) Parse(line string) (string, Point, error) {
	parts := strings.Split(line, ` `)

	if len(parts) >= 3 {
		if epoch, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
			if value, err := strconv.ParseFloat(parts[1], 32); err == nil {
				metricName := parts[0]
				metricName = strings.TrimSpace(metricName)

				return metricName, Point{
					Timestamp: time.Unix(epoch, 0),
					Value:     value,
				}, nil
			}
		}
	}

	return ``, Point{}, fmt.Errorf("Invalid Graphite metric line %q", line)
}

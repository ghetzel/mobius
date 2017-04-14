package mobius

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type KairosParser struct {
	Parser
}

func (self KairosParser) Parse(line string) (string, Point, error) {
	parts := strings.Split(line, ` `)

	if len(parts) >= 4 {
		if parts[0] == `put` {
			if epochMs, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
				if value, err := strconv.ParseFloat(parts[3], 32); err == nil {
					tags := parts[4:]
					metricName := parts[1]

					//  any tags are part of the metric name
					if len(tags) >= 0 {
						sort.Strings(tags)
						metricName = fmt.Sprintf("%s %s", metricName, strings.Join(tags, ` `))
					}

					metricName = strings.TrimSpace(metricName)

					return metricName, Point{
						Timestamp: time.Unix(0, epochMs*int64(time.Millisecond)),
						Value:     value,
					}, nil
				}
			}
		}
	}

	return ``, Point{}, fmt.Errorf("Invalid KairosDB metric line %q", line)
}

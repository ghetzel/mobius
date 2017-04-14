package mobius

import (
	"github.com/ghetzel/go-stockutil/stringutil"
	"strings"
	"time"
)

func ParseTimeString(timeval string) (time.Time, error) {
	if timeval == `` {
		return time.Now(), nil
	}

	if strings.HasPrefix(timeval, `-`) {
		if duration, err := time.ParseDuration(timeval); err == nil {
			return time.Now().Add(duration), nil
		} else {
			return time.Time{}, err
		}
	} else {
		if tm, err := stringutil.ConvertToTime(timeval); err == nil {
			return tm, nil
		} else {
			return time.Time{}, err
		}
	}
}

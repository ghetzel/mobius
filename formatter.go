package mobius

type Formatter interface {
	Format(*Metric, *Point) string
}

func GetFormatter(name string) (Formatter, bool) {
	switch name {
	case `graphite`:
		return CarbonFormatter{}, true
	case `kairosdb`:
		return KairosFormatter{}, true
	default:
		return nil, false
	}
}

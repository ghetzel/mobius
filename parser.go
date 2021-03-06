package mobius

type Parser interface {
	Parse(string) (string, Point, error)
}

func GetParser(name string) (Parser, bool) {
	switch name {
	case `graphite`:
		return CarbonParser{}, true
	case `kairosdb`:
		return KairosParser{}, true
	default:
		return nil, false
	}
}

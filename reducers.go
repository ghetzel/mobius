package mobius

import (
	"github.com/montanaflynn/stats"
	"math"
)

type statsUnary func(stats.Float64Data) (float64, error)
type ReducerFunc func(values ...float64) float64

// wraps a unary function from the stats package in our ReducerFunc
func statsFn(fn statsUnary) ReducerFunc {
	return func(values ...float64) float64 {
		if result, err := fn(stats.Float64Data(values)); err == nil {
			return result
		} else {
			return math.NaN()
		}
	}
}

func percentileFn(percentile float64) ReducerFunc {
	return func(values ...float64) float64 {
		if result, err := stats.Percentile(stats.Float64Data(values), percentile); err == nil {
			return result
		} else {
			return math.NaN()
		}
	}
}

var First = func(values ...float64) float64 {
	if len(values) == 0 {
		return 0
	} else {
		return values[0]
	}
}

var Last = func(values ...float64) float64 {
	if len(values) == 0 {
		return 0
	} else {
		return values[len(values)-1]
	}
}

var Count = func(values ...float64) float64 {
	return float64(len(values))
}

var GeometricMean = statsFn(stats.GeometricMean)
var HarmonicMean = statsFn(stats.HarmonicMean)
var InterQuartileRange = statsFn(stats.InterQuartileRange)
var Maximum = statsFn(stats.Max)
var Mean = statsFn(stats.Mean)
var Median = statsFn(stats.Median)
var MedianAbsoluteDeviation = statsFn(stats.MedianAbsoluteDeviation)
var MedianAbsoluteDeviationPopulation = statsFn(stats.MedianAbsoluteDeviationPopulation)
var Midhinge = statsFn(stats.Midhinge)
var Minimum = statsFn(stats.Min)
var Percent25 = percentileFn(0.25)
var Percent50 = percentileFn(0.50)
var Percent75 = percentileFn(0.75)
var Percent85 = percentileFn(0.85)
var Percent90 = percentileFn(0.90)
var Percent95 = percentileFn(0.95)
var Percent98 = percentileFn(0.98)
var Percent99 = percentileFn(0.99)
var Percent9999 = percentileFn(0.9999)
var PopulationVariance = statsFn(stats.PopulationVariance)
var SampleVariance = statsFn(stats.SampleVariance)
var StandardDeviation = statsFn(stats.StandardDeviation)
var StandardDeviationPopulation = statsFn(stats.StandardDeviationPopulation)
var StandardDeviationSample = statsFn(stats.StandardDeviationSample)
var Sum = statsFn(stats.Sum)
var Trimean = statsFn(stats.Trimean)
var Variance = statsFn(stats.Variance)

func Reduce(reducer ReducerFunc, values ...float64) float64 {
	switch len(values) {
	case 0:
		return 0
	default:
		return reducer(values...)
	}
}

var reducerNameMap = map[string]ReducerFunc{
	`count`:                                Count,
	`first`:                                First,
	`geometric-mean`:                       GeometricMean,
	`harmonic-mean`:                        HarmonicMean,
	`inter-quartile-range`:                 InterQuartileRange,
	`last`:                                 Last,
	`maximum`:                              Maximum,
	`mean`:                                 Mean,
	`median`:                               Median,
	`median-absolute-deviation`:            MedianAbsoluteDeviation,
	`median-absolute-deviation-population`: MedianAbsoluteDeviationPopulation,
	`midhinge`:                             Midhinge,
	`minimum`:                              Minimum,
	`percent25`:                            Percent25,
	`percent50`:                            Percent50,
	`percent75`:                            Percent75,
	`percent85`:                            Percent85,
	`percent90`:                            Percent90,
	`percent95`:                            Percent95,
	`percent98`:                            Percent98,
	`percent99`:                            Percent99,
	`percent9999`:                          Percent9999,
	`population-variance`:                  PopulationVariance,
	`sample-variance`:                      SampleVariance,
	`standard-deviation`:                   StandardDeviation,
	`standard-deviation-population`:        StandardDeviationPopulation,
	`standard-deviation-sample`:            StandardDeviationSample,
	`sum`:      Sum,
	`trimean`:  Trimean,
	`variance`: Variance,
}

var reducerAliasMap = map[string]string{
	`gmean`:     `geometric-mean`,
	`hmean`:     `harmonic-mean`,
	`iqr`:       `inter-quartile-range`,
	`max`:       `maximum`,
	`avg`:       `mean`,
	`average`:   `mean`,
	`mad`:       `median-absolute-deviation`,
	`madp`:      `median-absolute-deviation-population`,
	`pvar`:      `population-variance`,
	`svar`:      `sample-variance`,
	`stddev`:    `standard-deviation`,
	`stddevp`:   `standard-deviation-population`,
	`stddevs`:   `standard-deviation-sample`,
	`lower25`:   `percent25`,
	`upper75`:   `percent75`,
	`upper85`:   `percent85`,
	`upper90`:   `percent90`,
	`upper95`:   `percent95`,
	`upper98`:   `percent98`,
	`upper99`:   `percent99`,
	`upper9999`: `percent9999`,
	`var`:       `variance`,
}

func GetReducer(name string) (ReducerFunc, bool) {
	if reducer, ok := reducerNameMap[GetReducerName(name)]; ok {
		return reducer, true
	}

	return nil, false
}

func GetReducerName(aliasOrName string) string {
	if _, ok := reducerNameMap[aliasOrName]; ok {
		return aliasOrName
	} else if alias, ok := reducerAliasMap[aliasOrName]; ok {
		if _, ok := reducerNameMap[alias]; ok {
			return alias
		}
	}

	return ``
}

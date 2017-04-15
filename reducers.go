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
var Max = statsFn(stats.Max)
var Mean = statsFn(stats.Mean)
var Median = statsFn(stats.Median)
var MedianAbsoluteDeviation = statsFn(stats.MedianAbsoluteDeviation)
var MedianAbsoluteDeviationPopulation = statsFn(stats.MedianAbsoluteDeviationPopulation)
var Midhinge = statsFn(stats.Midhinge)
var Min = statsFn(stats.Min)
var PopulationVariance = statsFn(stats.PopulationVariance)
var SampleVariance = statsFn(stats.SampleVariance)
var StandardDeviation = statsFn(stats.StandardDeviation)
var StandardDeviationPopulation = statsFn(stats.StandardDeviationPopulation)
var StandardDeviationSample = statsFn(stats.StandardDeviationSample)
var Sum = statsFn(stats.Sum)
var Trimean = statsFn(stats.Trimean)
var Variance = statsFn(stats.Variance)

// aliases, because typing gets annoying sometimes
var GMean = GeometricMean
var HMean = HarmonicMean
var IQR = InterQuartileRange
var MAD = MedianAbsoluteDeviation
var MADP = MedianAbsoluteDeviationPopulation
var PVar = PopulationVariance
var StdDev = StandardDeviation
var StdDevP = StandardDeviationPopulation
var StdDevS = StandardDeviationSample
var SVar = SampleVariance

func Reduce(reducer ReducerFunc, values ...float64) float64 {
	switch len(values) {
	case 0:
		return 0
	default:
		return reducer(values...)
	}
}

func GetReducer(name string) (ReducerFunc, bool) {
	switch name {
	case `count`:
		return Count, true
	case `first`:
		return First, true
	case `gmean`, `geometric-mean`:
		return GeometricMean, true
	case `hmean`, `harmonic-mean`:
		return HarmonicMean, true
	case `iqr`, `inter-quartile-range`:
		return InterQuartileRange, true
	case `last`:
		return Last, true
	case `max`, `maximum`:
		return Max, true
	case `mean`, `avg`, `average`:
		return Mean, true
	case `median`:
		return Median, true
	case `mad`, `media-absolute-deviation`:
		return MedianAbsoluteDeviation, true
	case `madp`, `media-absolute-deviation-population`:
		return MedianAbsoluteDeviationPopulation, true
	case `midhinge`:
		return Midhinge, true
	case `min`, `minimum`:
		return Min, true
	case `pvar`, `population-variance`:
		return PopulationVariance, true
	case `svar`, `sample-variance`:
		return SampleVariance, true
	case `stddev`, `standard-deviation`:
		return StandardDeviation, true
	case `stddevp`, `standard-deviation-population`:
		return StandardDeviationPopulation, true
	case `stddevs`, `standard-deviation-sample`:
		return StandardDeviationSample, true
	case `sum`:
		return Sum, true
	case `trimean`:
		return Trimean, true
	case `var`, `variance`:
		return Variance, true
	}

	return nil, false
}

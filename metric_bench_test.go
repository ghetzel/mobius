package mobius

import (
	"math/rand"
	"testing"
	"time"
)

func benchmarkBucketing(b *testing.B, count int, duration time.Duration) {
	metric := NewMetric(`mobius.bench.pointbucket`)

	for i := 0; i < count; i++ {
		metric.Push(&Point{
			Timestamp: time.Date(2006, 1, 2, 15, 4, i, 0, mst),
			Value:     0.1 + float64(rand.Float64()*float64(i)),
		})
	}

	input := metric.GetPoints()

	for n := 0; n < b.N; n++ {
		MakeTimeBuckets(input, duration)
	}
}

func BenchmarkBucket_1_30s(b *testing.B) {
	benchmarkBucketing(b, 1, 30*time.Second)
}

func BenchmarkBucket_10_30s(b *testing.B) {
	benchmarkBucketing(b, 10, 30*time.Second)
}

func BenchmarkBucket_100_30s(b *testing.B) {
	benchmarkBucketing(b, 100, 30*time.Second)
}

func BenchmarkBucket_1000_30s(b *testing.B) {
	benchmarkBucketing(b, 1000, 30*time.Second)
}

func BenchmarkBucket_10000_30s(b *testing.B) {
	benchmarkBucketing(b, 10000, 30*time.Second)
}

func BenchmarkBucket_100000_30s(b *testing.B) {
	benchmarkBucketing(b, 100000, 30*time.Second)
}

func BenchmarkBucket_1_5m(b *testing.B) {
	benchmarkBucketing(b, 1, 5*time.Minute)
}

func BenchmarkBucket_10_5m(b *testing.B) {
	benchmarkBucketing(b, 10, 5*time.Minute)
}

func BenchmarkBucket_100_5m(b *testing.B) {
	benchmarkBucketing(b, 100, 5*time.Minute)
}

func BenchmarkBucket_1000_5m(b *testing.B) {
	benchmarkBucketing(b, 1000, 5*time.Minute)
}

func BenchmarkBucket_10000_5m(b *testing.B) {
	benchmarkBucketing(b, 10000, 5*time.Minute)
}

func BenchmarkBucket_100000_5m(b *testing.B) {
	benchmarkBucketing(b, 100000, 5*time.Minute)
}

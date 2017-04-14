package mobius

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func benchmarkRange(b *testing.B, count int, tags string, parallel bool) {
	tempPath, err := ioutil.TempDir(``, `mobius_test_`)
	if err != nil {
		panic(err.Error())
	}

	defer os.RemoveAll(tempPath)

	database, err := OpenDataset(tempPath)
	if err != nil {
		panic(err.Error())
	}

	event1 := NewMetric(`mobius.test.bench1` + tags)

	for i := 0; i < count; i++ {
		event1.Push(time.Date(2006, 1, 2, 15, 4, 5+i, 0, mst), float64(1.2*float64(i+1)))
	}

	if err := database.Write(event1); err != nil {
		panic(fmt.Sprintf("Error writing %s: %v", event1.GetName(), err))
	}

	fn := func() {
		if _, err := database.Range(time.Time{}, time.Now(), `mobius.test.bench1`); err != nil {
			panic(err.Error())
		}
	}

	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				fn()
			}
		})
	} else {
		for n := 0; n < b.N; n++ {
			fn()
		}
	}
}

func BenchmarkRange_1(b *testing.B) {
	benchmarkRange(b, 1, ``, false)
}

func BenchmarkRange_10(b *testing.B) {
	benchmarkRange(b, 10, ``, false)
}

func BenchmarkRange_100(b *testing.B) {
	benchmarkRange(b, 100, ``, false)
}

func BenchmarkRange_1000(b *testing.B) {
	benchmarkRange(b, 1000, ``, false)
}

func BenchmarkRange_10000(b *testing.B) {
	benchmarkRange(b, 10000, ``, false)
}

func BenchmarkRange_100000(b *testing.B) {
	benchmarkRange(b, 100000, ``, false)
}

func BenchmarkRangeWithTags_1(b *testing.B) {
	benchmarkRange(b, 1, `,test=true,zzyxx=1.2,factor=3`, false)
}

func BenchmarkRangeWithTags_10(b *testing.B) {
	benchmarkRange(b, 10, `,test=true,zzyxx=1.2,factor=3`, false)
}

func BenchmarkRangeWithTags_100(b *testing.B) {
	benchmarkRange(b, 100, `,test=true,zzyxx=1.2,factor=3`, false)
}

func BenchmarkRangeWithTags_1000(b *testing.B) {
	benchmarkRange(b, 1000, `,test=true,zzyxx=1.2,factor=3`, false)
}

func BenchmarkRangeWithTags_10000(b *testing.B) {
	benchmarkRange(b, 10000, `,test=true,zzyxx=1.2,factor=3`, false)
}

func BenchmarkRangeWithTags_100000(b *testing.B) {
	benchmarkRange(b, 100000, `,test=true,zzyxx=1.2,factor=3`, false)
}

func BenchmarkRange_P1(b *testing.B) {
	benchmarkRange(b, 1, ``, true)
}

func BenchmarkRange_P10(b *testing.B) {
	benchmarkRange(b, 10, ``, true)
}

func BenchmarkRange_P100(b *testing.B) {
	benchmarkRange(b, 100, ``, true)
}

func BenchmarkRange_P1000(b *testing.B) {
	benchmarkRange(b, 1000, ``, true)
}

func BenchmarkRange_P10000(b *testing.B) {
	benchmarkRange(b, 10000, ``, true)
}

func BenchmarkRange_P100000(b *testing.B) {
	benchmarkRange(b, 100000, ``, true)
}

func BenchmarkRangeWithTags_P1(b *testing.B) {
	benchmarkRange(b, 1, `,test=true,zzyxx=1.2,factor=3`, true)
}

func BenchmarkRangeWithTags_P10(b *testing.B) {
	benchmarkRange(b, 10, `,test=true,zzyxx=1.2,factor=3`, true)
}

func BenchmarkRangeWithTags_P100(b *testing.B) {
	benchmarkRange(b, 100, `,test=true,zzyxx=1.2,factor=3`, true)
}

func BenchmarkRangeWithTags_P1000(b *testing.B) {
	benchmarkRange(b, 1000, `,test=true,zzyxx=1.2,factor=3`, true)
}

func BenchmarkRangeWithTags_P10000(b *testing.B) {
	benchmarkRange(b, 10000, `,test=true,zzyxx=1.2,factor=3`, true)
}

func BenchmarkRangeWithTags_P100000(b *testing.B) {
	benchmarkRange(b, 100000, `,test=true,zzyxx=1.2,factor=3`, true)
}

package mobius

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"path"
	"time"
)

func benchmarkRange(b *testing.B, count int, parallel bool) {
	tempPath, err := ioutil.TempDir(``, `mobius_test_`)
	if err != nil {
		panic(err.Error())
	}

	defer os.RemoveAll(tempPath)

	database, err := OpenDataset(path.Join(tempPath, `test.db`))
	if err != nil {
		panic(err.Error())
	}

	event1 := NewMetric(`mobius.test.bench1`)

	for i := 0; i < count; i++ {
		if err := database.Write(event1, &Point{
			Timestamp: time.Date(2006, 1, 2, 15, 4, 5+i, 0, mst),
			Value: float64(1.2 * float64(i+1)),
		}); err != nil {
			panic(fmt.Sprintf("Error writing %s[%d]: %v", event1.Name, i, err))
		}
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
	benchmarkRange(b, 1, false)
}

func BenchmarkRange_10(b *testing.B) {
	benchmarkRange(b, 10, false)
}

func BenchmarkRange_100(b *testing.B) {
	benchmarkRange(b, 100, false)
}

func BenchmarkRange_1000(b *testing.B) {
	benchmarkRange(b, 1000, false)
}

func BenchmarkRange_10000(b *testing.B) {
	benchmarkRange(b, 10000, false)
}

func BenchmarkRange_100000(b *testing.B) {
	benchmarkRange(b, 100000, false)
}

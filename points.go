package mobius

import (
	"fmt"
	"sort"
	"time"
)

type Point struct {
	Timestamp time.Time `json:"time"`
	Value     float64   `json:"value"`
}

func (self Point) String() string {
	return fmt.Sprintf("(%v, %f)", self.Timestamp, self.Value)
}

type PointSet []Point

func (self PointSet) Timestamps() []time.Time {
	output := make([]time.Time, len(self))

	for i, point := range self {
		output[i] = point.Timestamp
	}

	return output
}

func (self PointSet) Oldest() *Point {
	if l := len(self); l == 0 {
		return nil
	} else {
		return &self[0]
	}
}

func (self PointSet) Newest() *Point {
	if l := len(self); l == 0 {
		return nil
	} else {
		return &self[l-1]
	}
}

func (self PointSet) Values() []float64 {
	output := make([]float64, len(self))

	for i, point := range self {
		output[i] = point.Value
	}

	return output
}

func (self PointSet) Len() int {
	return len(self)
}

func (self PointSet) Less(i, j int) bool {
	if l := self.Len(); i < l && j < l {
		return self[i].Timestamp.Before(self[j].Timestamp)
	}

	return false
}

func (self PointSet) Swap(i, j int) {
	if l := self.Len(); i < l && j < l {
		temp := self[i]
		self[i] = self[j]
		self[j] = temp
	}
}

func MakeTimeBuckets(points PointSet, duration time.Duration) []PointSet {
	pointsets := make([]PointSet, 0)

	if l := len(points); l > 0 {
		currentBucket := make(PointSet, 0)
		endOfBucket := points[l-1].Timestamp.Add(-1 * duration)

		for i := (l - 1); i >= 0; i-- {
			currentBucket = append(PointSet{points[i]}, currentBucket...)

			if i > 0 {
				if t := points[i-1].Timestamp; t.Before(endOfBucket) || t.Equal(endOfBucket) {
					if len(currentBucket) > 0 {
						sort.Sort(currentBucket)
						pointsets = append([]PointSet{currentBucket}, pointsets...)
					}

					currentBucket = nil

					endOfBucket = t.Add(-1 * duration)
				}
			}
		}

		if len(currentBucket) > 0 {
			sort.Sort(currentBucket)
			pointsets = append([]PointSet{currentBucket}, pointsets...)
			currentBucket = nil
		}
	}

	return pointsets
}

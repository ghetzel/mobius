package mobius

import (
	"fmt"
	"github.com/gobwas/glob"
	"github.com/op/go-logging"
	"github.com/tidwall/buntdb"
	"github.com/wcharczuk/go-chart"
	"strconv"
	"strings"
	"time"
)

var log = logging.MustGetLogger(`mobius/db`)

type Dataset struct {
	StoreZeroes bool
	filename    string
	db          *buntdb.DB
}

func OpenDataset(filename string) (*Dataset, error) {
	out := &Dataset{
		filename: filename,
	}

	if conn, err := buntdb.Open(out.filename); err == nil {
		out.db = conn
	} else {
		return nil, err
	}

	return out, nil
}

func (self *Dataset) GetNames(pattern string) ([]string, error) {
	if matcher, err := glob.Compile(pattern, '.'); err == nil {
		names := make([]string, 0)

		if err := self.db.View(func(tx *buntdb.Tx) error {
			return tx.AscendKeys(`metrics:*:id`, func(key, value string) bool {
				key = strings.TrimPrefix(key, `metrics:`)
				key = strings.TrimSuffix(key, `:id`)

				if matcher.Match(key) {
					names = append(names, key)
				}

				return true
			})
		}); err != nil {
			return nil, err
		}

		return names, nil
	} else {
		return nil, err
	}
}

func (self *Dataset) Range(start time.Time, end time.Time, names ...string) (map[string][]*Point, error) {
	data := make(map[string][]*Point)

	if start.IsZero() {
		start = time.Unix(0, 0)
	}

	if end.IsZero() {
		end = time.Now()
	}

	if err := self.db.View(func(tx *buntdb.Tx) error {
		for _, name := range names {
			points := make([]*Point, 0)
			prefix := fmt.Sprintf("metrics:%s:values:", name)
			skey := fmt.Sprintf("%s%v", prefix, start.UnixNano())
			ekey := fmt.Sprintf("%s%v", prefix, end.UnixNano())

			if err := tx.AscendRange(``, skey, ekey, func(key, value string) bool {
				timestamp := key[len(prefix):]

				if epoch_ns, err := strconv.ParseInt(timestamp, 10, 64); err == nil {
					tm := time.Unix(0, epoch_ns)

					if val, err := strconv.ParseFloat(value, 64); err == nil {
						points = append(points, &Point{
							Timestamp: tm,
							Value:     val,
						})
					} else {
						log.Errorf("value parse error: %v", err)
					}
				} else {
					log.Errorf("epoch parse error %s: %v", key, err)
				}

				return true
			}); err != nil {
				return err
			}

			data[name] = points
		}

		return nil
	}); err == nil {
		return data, nil
	} else {
		return data, err
	}
}

func (self *Dataset) Write(name string, point Point) error {
	if self.StoreZeroes || point.Value != 0 {
		return self.db.Update(func(tx *buntdb.Tx) error {
			idKey := fmt.Sprintf("metrics:%s:id", name)
			ts := fmt.Sprintf("%v", point.Timestamp.UnixNano())
			valueKey := fmt.Sprintf("metrics:%s:values:%s", name, ts)
			value := fmt.Sprintf("%g", point.Value)

			tx.CreateIndex("metrics", "metrics:*:id", buntdb.IndexString)

			if _, _, err := tx.Set(idKey, name, nil); err == nil {
				if _, _, err := tx.Set(valueKey, value, nil); err == nil {
					return nil
				} else {
					return err
				}
			} else {
				return err
			}
		})
	}

	return nil
}

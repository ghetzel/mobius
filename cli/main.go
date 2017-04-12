package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/ghetzel/cli"
	"github.com/ghetzel/go-stockutil/stringutil"
	"github.com/ghetzel/mobius"
	"github.com/op/go-logging"
	"github.com/wcharczuk/go-chart"
	"os"
	"strings"
	"time"
)

var DefaultRenderFormat = `json`
var DefaultParser = `kairosdb`
var log = logging.MustGetLogger(`main`)

func main() {
	app := cli.NewApp()
	app.Name = `mobius`
	app.Usage = `Storing and rendering time-series data, forever and ever.`
	app.Version = `0.0.1`
	app.EnableBashCompletion = false

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   `log-level, L`,
			Usage:  `Level of log output verbosity`,
			Value:  `debug`,
			EnvVar: `LOGLEVEL`,
		},
	}

	app.Before = func(c *cli.Context) error {
		logging.SetFormatter(logging.MustStringFormatter(`%{color}%{level:.4s}%{color:reset}[%{id:04d}] %{message}`))

		if level, err := logging.LogLevel(c.String(`log-level`)); err == nil {
			logging.SetLevel(level, ``)
		} else {
			return err
		}

		return nil
	}

	app.Commands = []cli.Command{
		{
			Name:      `push`,
			ArgsUsage: `PATH`,
			Usage:     `Push time series observations into the named dataset as read from standard input.`,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  `parser, p`,
					Usage: `The parser to use for decoding input data.`,
					Value: DefaultParser,
				},
			},
			Action: func(c *cli.Context) {
				if parser, ok := mobius.GetParser(c.String(`parser`)); ok {
					if dataset, err := mobius.OpenDataset(c.Args().First()); err == nil {
						scanner := bufio.NewScanner(os.Stdin)

						for scanner.Scan() {
							if err := scanner.Err(); err != nil {
								log.Fatalf("Error reading input: %v", err)
							}

							line := scanner.Text()

							if name, point, err := parser.Parse(line); err == nil {
								metric := mobius.NewMetric(name)

								if err := dataset.Write(metric, point); err != nil {
									log.Fatalf("write failed: %v", err)
								}
							} else {
								log.Warningf("malformed line: %v", err)
							}
						}
					} else {
						log.Fatalf("Must specify a dataset path.")
					}
				} else {
					log.Fatalf("Unknown parser %q", parser)
				}
			},
		}, {
			Name:      `query`,
			ArgsUsage: `PATH [SERIES ..]`,
			Usage:     `Query the named dataset and output the results in a given format.`,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  `format, f`,
					Usage: `The output format to render the data into.`,
					Value: DefaultRenderFormat,
				},
				cli.StringFlag{
					Name:  `start-time, s`,
					Usage: `The start time for retrieving data.`,
					Value: `-1h`,
				},
				cli.StringFlag{
					Name:  `end-time, e`,
					Usage: `The end time for retrieving data.`,
					Value: ``,
				},
			},
			Action: func(c *cli.Context) {
				if c.NArg() > 1 {
					start := parseTimeFlag(c.String(`start-time`))
					end := parseTimeFlag(c.String(`end-time`))

					if dataset, err := mobius.OpenDataset(c.Args().First()); err == nil {
						names := make([]string, 0)
						patterns := c.Args()[1:]

						for _, pattern := range patterns {
							if nm, err := dataset.GetNames(pattern); err == nil {
								names = append(names, nm...)
							} else {
								log.Errorf("Invalid name pattern %q: %v", pattern, err)
							}
						}

						if metrics, err := dataset.Range(start, end, names...); err == nil {
							format := c.String(`format`)

							switch format {
							case `png`, `svg`:
								graph := chart.Chart{
									Series: make([]chart.Series, 0),
								}

								for _, metric := range metrics {
									series := chart.ContinuousSeries{
										YValueFormatter: chart.TimeValueFormatter,
										XValues:         make([]float64, 0),
										YValues:         make([]float64, 0),
									}

									for _, point := range metric.Points {
										series.XValues = append(series.XValues, float64(point.Timestamp.UnixNano()))
										series.YValues = append(series.YValues, point.Value)
									}

									graph.Series = append(graph.Series, series)
								}

								if err := graph.Render(chart.PNG, os.Stdout); err != nil {
									log.Fatalf("Failed to render: %v", err)
								}
							case `json`:
								enc := json.NewEncoder(os.Stdout)
								enc.SetIndent(``, `  `)

								if err := enc.Encode(metrics); err != nil {
									log.Fatal(err)
								}

							default:
								if formatter, ok := mobius.GetFormatter(format); ok {
									for _, metric := range metrics {
										for _, point := range metric.Points {
											fmt.Println(formatter.Format(metric, point))
										}
									}
								} else {
									log.Fatalf("Unknown formatter %q", format)
								}
							}
						} else {
							log.Fatalf("Query failed: %v", err)
						}
					} else {
						log.Fatalf("Failed to open dataset: %v", err)
					}
				} else {
					log.Fatalf("Must specify a dataset path and at least one series to retrieve.")
				}
			},
		},
	}

	app.Run(os.Args)
}

func parseTimeFlag(timeval string) time.Time {
	if timeval == `` {
		return time.Now()
	}

	if strings.HasPrefix(timeval, `-`) {
		if duration, err := time.ParseDuration(timeval); err == nil {
			return time.Now().Add(duration)
		} else {
			log.Fatal(err)
			return time.Time{}
		}
	} else {
		if tm, err := stringutil.ConvertToTime(timeval); err == nil {
			return tm
		} else {
			log.Fatal(err)
			return time.Time{}
		}
	}
}

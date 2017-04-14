package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/ghetzel/cli"
	"github.com/ghetzel/mobius"
	"github.com/op/go-logging"
	"os"
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
						defer dataset.Close()

						scanner := bufio.NewScanner(os.Stdin)

						for scanner.Scan() {
							if err := scanner.Err(); err != nil {
								log.Fatalf("Error reading input: %v", err)
							}

							line := scanner.Text()

							if name, point, err := parser.Parse(line); err == nil {
								metric := mobius.NewMetric(name)
								metric.PushPoint(point)

								if err := dataset.Write(metric); err != nil {
									log.Fatalf("write failed: %v", err)
								}
							} else {
								log.Warningf("malformed line: %v", err)
							}
						}
					} else {
						log.Fatalf("Failed to open dataset: %v", err)
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
				cli.StringFlag{
					Name:  `graph-title, T`,
					Usage: `The title of the graph.`,
				},
			},
			Action: func(c *cli.Context) {
				if c.NArg() > 1 {
					start, err := mobius.ParseTimeString(c.String(`start-time`))
					if err != nil {
						log.Fatalf("Invalid start time: %v", err)
					}

					end, err := mobius.ParseTimeString(c.String(`end-time`))
					if err != nil {
						log.Fatalf("Invalid end time: %v", err)
					}

					if dataset, err := mobius.OpenDatasetReadOnly(c.Args().First()); err == nil {
						defer dataset.Close()
						patterns := c.Args()[1:]

						if metrics, err := dataset.Range(start, end, patterns...); err == nil {
							format := c.String(`format`)

							switch format {
							case `png`, `svg`:
								graph := mobius.NewGraph(metrics)

								if v := c.String(`graph-title`); v != `` {
									graph.Options.Title = v
									graph.Style.Title.Show = true
								}

								if err := graph.Render(os.Stdout, mobius.RenderFormat(format)); err != nil {
									log.Fatalf("Graph render error: %v", err)
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
										for _, point := range metric.Points() {
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
		}, {
			Name:      `ls`,
			ArgsUsage: `PATH [METRICS ..]`,
			Usage:     "List metric names from the dataset.",
			Action: func(c *cli.Context) {
				if dataset, err := mobius.OpenDataset(c.Args().First()); err == nil {
					defer dataset.Close()
					pattern := c.Args().Get(1)

					if pattern == `` {
						pattern = `**`
					}

					if names, err := dataset.GetNames(pattern); err == nil {
						for _, name := range names {
							fmt.Println(name)
						}
					} else {
						log.Fatalf("Failed to retrieve names: %v", err)
					}
				} else {
					log.Fatalf("Failed to open dataset: %v", err)
				}
			},
		}, {
			Name:      `rm`,
			ArgsUsage: `PATH METRICS`,
			Usage:     `Remove metrics from the given dataset.`,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  `older-than, B`,
					Usage: `Remove points older than the given duration or time.`,
				},
				cli.StringFlag{
					Name:  `newer-than, A`,
					Usage: `Remove points newer than the given duration or time.`,
				},
			},
			Action: func(c *cli.Context) {
				if c.NArg() > 0 {
					var before, after time.Time
					var err error

					if v := c.String(`older-than`); v != `` {
						before, err = mobius.ParseTimeString(v)

						if err != nil {
							log.Fatalf("Invalid time: %v", err)
						}
					}

					if v := c.String(`newer-than`); v != `` {
						after, err = mobius.ParseTimeString(v)

						if err != nil {
							log.Fatalf("Invalid time: %v", err)
						}
					}

					if dataset, err := mobius.OpenDataset(c.Args().First()); err == nil {
						defer dataset.Close()
						patterns := c.Args()[1:]

						if before.IsZero() && after.IsZero() {
							if n, err := dataset.Remove(patterns...); err == nil {
								log.Noticef("Removed %d metrics", n)
							} else {
								log.Fatalf("Failed to remove metrics: %v", err)
							}
						} else {
							if !before.IsZero() {
								if n, err := dataset.TrimBefore(before, patterns...); err == nil {
									log.Noticef("Removed %d points older than %v", n, before)
								} else {
									log.Fatalf("Failed to remove points: %v", err)
								}
							}

							if !after.IsZero() {
								if n, err := dataset.TrimAfter(after, patterns...); err == nil {
									log.Noticef("Removed %d points newer than %v", n, after)
								} else {
									log.Fatalf("Failed to remove points: %v", err)
								}
							}
						}
					} else {
						log.Fatalf("Failed to open dataset: %v", err)
					}
				} else {
					log.Fatalf("Must specify a dataset path and at least one series to remove.")
				}
			},
		}, {
			Name:      `compact`,
			ArgsUsage: `PATH`,
			Usage:     `Compact the given dataset.`,
			Action: func(c *cli.Context) {
				if dataset, err := mobius.OpenDataset(c.Args().First()); err == nil {
					defer dataset.Close()

					if err := dataset.Compact(); err != nil {
						log.Fatalf("Failed to compact dataset: %v", err)
					}
				} else {
					log.Fatalf("Failed to open dataset: %v", err)
				}
			},
		}, {
			Name:      `backup`,
			ArgsUsage: `PATH`,
			Usage:     `Dump a restorable backup of the dataset to standard output.`,
			Action: func(c *cli.Context) {
				if dataset, err := mobius.OpenDataset(c.Args().First()); err == nil {
					defer dataset.Close()

					if err := dataset.Backup(os.Stdout); err != nil {
						log.Fatalf("Failed to backup dataset: %v", err)
					}
				} else {
					log.Fatalf("Failed to open dataset: %v", err)
				}
			},
		}, {
			Name:      `restore`,
			ArgsUsage: `PATH`,
			Usage:     "Restore a backup of the dataset from standard input (ALL EXISTING DATA WILL BE DESTROYED.)",
			Action: func(c *cli.Context) {
				if dataset, err := mobius.OpenDataset(c.Args().First()); err == nil {
					defer dataset.Close()

					if err := dataset.Restore(os.Stdin); err != nil {
						log.Fatalf("Failed to restore dataset: %v", err)
					}
				} else {
					log.Fatalf("Failed to open dataset: %v", err)
				}
			},
		},
	}

	app.Run(os.Args)
}

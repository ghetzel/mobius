package main

import (
	"github.com/ghetzel/cli"
	"github.com/op/go-logging"
	"os"
)

var DefaultRenderFormat = `json`
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
			Action: func(c *cli.Context) {

			},
		}, {
			Name:      `query`,
			ArgsUsage: `PATHS [SERIES ..]`,
			Usage:     `Query the named dataset and output the results in a given format.`,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  `format, f`,
					Usage: `The output format to render the data into.`,
					Value: DefaultRenderFormat,
				},
			},
			Action: func(c *cli.Context) {

			},
		},
	}

	app.Run(os.Args)
}

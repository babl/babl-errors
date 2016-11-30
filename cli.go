package main

import (
	"github.com/urfave/cli"
)

func configureCli() (app *cli.App) {
	app = cli.NewApp()
	app.Usage = "babl-errors"
	app.Version = Version
	app.Action = func(c *cli.Context) {
		run(c.String("kafka-brokers"), c.GlobalBool("debug"))
	}
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "kafka-brokers, kb",
			Usage: "Comma separated list of kafka brokers",
			Value: "127.0.0.1:9092",
		},
		cli.BoolFlag{
			Name:   "debug",
			Usage:  "Enable debug mode & verbose logging",
			EnvVar: "BABL_DEBUG",
		},
	}
	return
}

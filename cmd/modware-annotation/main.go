package main

import (
	"os"

	apiflag "github.com/dictyBase/apihelpers/command/flag"
	arangoflag "github.com/dictyBase/arangomanager/command/flag"
	oboaction "github.com/dictyBase/go-obograph/command/action"
	oboflag "github.com/dictyBase/go-obograph/command/flag"
	obovalidate "github.com/dictyBase/go-obograph/command/validate"
	"github.com/dictyBase/modware-annotation/internal/app/server"
	"github.com/dictyBase/modware-annotation/internal/app/validate"
	cli "gopkg.in/urfave/cli.v1"
)

func main() {
	app := cli.NewApp()
	app.Name = "modware-annotation"
	app.Usage = "cli for modware-annotation microservice"
	app.Version = "1.0.0"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "log-format",
			Usage: "format of the logging out, either of json or text.",
			Value: "json",
		},
		cli.StringFlag{
			Name:  "log-level",
			Usage: "log level for the application",
			Value: "error",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:   "start-server",
			Usage:  "starts the modware-annotation microservice with grpc backends",
			Action: server.RunServer,
			Before: validate.ValidateServerArgs,
			Flags:  getServerFlags(),
		},
		{
			Name:   "load-ontologies",
			Usage:  "load one or more ontologies in obograph json format",
			Action: oboaction.LoadOntologies,
			Before: obovalidate.OntologyArgs,
			Flags:  oboflag.OntologyFlags(),
		},
	}
	app.Run(os.Args)
}

func getServerFlags() []cli.Flag {
	var f []cli.Flag
	f = append(
		f,
		[]cli.Flag{
			cli.StringFlag{
				Name:  "port",
				Usage: "tcp port at which the server will be available",
				Value: "9560",
			},
			cli.StringFlag{
				Name:  "term-collection",
				Usage: "arangodb collection for storing ontoloy terms",
				Value: "cvterm",
			},
			cli.StringFlag{
				Name:  "rel-collection",
				Usage: "arangodb collection for storing cvterm relationships",
				Value: "cvterm_relationship",
			},
			cli.StringFlag{
				Name:  "cv-collection",
				Usage: "arangodb collection for storing ontology information",
				Value: "cv",
			},
			cli.StringFlag{
				Name:  "obograph",
				Usage: "arangodb named graph for managing ontology graph",
				Value: "obograph",
			},
			cli.StringFlag{
				Name:  "anno-collection",
				Usage: "arangodb collection for storing annotations",
				Value: "annotation",
			},
			cli.StringFlag{
				Name:  "annoterm-collection",
				Usage: "arangodb edge collection for storing links between annotation and ontology term",
				Value: "annotation_cvterm",
			},
			cli.StringFlag{
				Name:  "annover-collection",
				Usage: "arangodb edge collection to link different versions of annotation",
				Value: "annotation_version",
			},
			cli.StringFlag{
				Name:  "annogroup-collection",
				Usage: "arangodb collection for storing annotation group",
				Value: "annotation_group",
			},
			cli.StringFlag{
				Name:  "annoterm-graph",
				Usage: "arangodb named graph for managing relations between annotation and ontology term",
				Value: "annotation_tag",
			},
			cli.StringFlag{
				Name:  "annover-graph",
				Usage: "arangodb named graph for managing relations betweens different versions of annotation",
				Value: "annotation_history",
			},
		}...,
	)
	f = append(f, arangoflag.ArangodbFlags()...)
	f = append(f, apiflag.NatsFlag()...)
	return f
}

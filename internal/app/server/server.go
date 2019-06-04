package server

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/dictyBase/apihelpers/aphgrpc"
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/modware-annotation/internal/app/service"
	"github.com/dictyBase/modware-annotation/internal/message/nats"
	"github.com/dictyBase/modware-annotation/internal/repository/arangodb"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	gnats "github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"gopkg.in/urfave/cli.v1"
)

func RunServer(c *cli.Context) error {
	arPort, _ := strconv.Atoi(c.String("arangodb-port"))
	connP := &manager.ConnectParams{
		User:     c.String("arangodb-user"),
		Pass:     c.String("arangodb-pass"),
		Database: c.String("arangodb-database"),
		Host:     c.String("arangodb-host"),
		Port:     arPort,
		Istls:    c.Bool("is-secure"),
	}
	collP := &arangodb.CollectionParams{
		Term:         c.String("term-collection"),
		Relationship: c.String("rel-collection"),
		GraphInfo:    c.String("cv-collection"),
		OboGraph:     c.String("obograph"),
		Annotation:   c.String("anno-collection"),
		AnnoTerm:     c.String("annoterm-collection"),
		AnnoVersion:  c.String("annover-collection"),
		AnnoTagGraph: c.String("annoterm-graph"),
		AnnoVerGraph: c.String("annover-graph"),
	}
	anrepo, err := arangodb.NewTaggedAnnotationRepo(connP, collP)
	if err != nil {
		return cli.NewExitError(
			fmt.Sprintf("cannot connect to arangodb annotation repository %s", err.Error()),
			2,
		)
	}
	ms, err := nats.NewPublisher(
		c.String("nats-host"),
		c.String("nats-port"),
		gnats.MaxReconnects(-1),
		gnats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return cli.NewExitError(
			fmt.Sprintf("cannot connect to messaging server %s", err.Error()),
			2,
		)
	}
	grpcS := grpc.NewServer(
		grpc_middleware.WithUnaryServerChain(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_logrus.UnaryServerInterceptor(getLogger(c)),
		),
	)
	annotation.RegisterTaggedAnnotationServiceServer(
		grpcS,
		service.NewAnnotationService(
			anrepo,
			ms,
			"groups",
			aphgrpc.TopicsOption(
				map[string]string{
					"annotationCreate": "AnnotationService.Create",
					"annotationDelete": "AnnotationService.Delete",
					"annotationUpdate": "AnnotationService.Update",
				}),
		),
	)
	reflection.Register(grpcS)

	// create listener
	endP := fmt.Sprintf(":%s", c.String("port"))
	lis, err := net.Listen("tcp", endP)
	if err != nil {
		return cli.NewExitError(
			fmt.Sprintf("failed to listen %s", err),
			2,
		)
	}
	log.Printf("starting grpc server on %s", endP)
	grpcS.Serve(lis)
	return nil
}

func getLogger(c *cli.Context) *logrus.Entry {
	log := logrus.New()
	log.Out = os.Stderr
	switch c.GlobalString("log-format") {
	case "text":
		log.Formatter = &logrus.TextFormatter{
			TimestampFormat: "02/Jan/2006:15:04:05",
		}
	case "json":
		log.Formatter = &logrus.JSONFormatter{
			TimestampFormat: "02/Jan/2006:15:04:05",
		}
	}
	l := c.GlobalString("log-level")
	switch l {
	case "debug":
		log.Level = logrus.DebugLevel
	case "warn":
		log.Level = logrus.WarnLevel
	case "error":
		log.Level = logrus.ErrorLevel
	case "fatal":
		log.Level = logrus.FatalLevel
	case "panic":
		log.Level = logrus.PanicLevel
	}
	return logrus.NewEntry(log)
}

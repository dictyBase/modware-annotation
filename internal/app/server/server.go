package server

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/dictyBase/aphgrpc"
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	ontoarango "github.com/dictyBase/go-obograph/storage/arangodb"
	"github.com/dictyBase/modware-annotation/internal/app/service"
	"github.com/dictyBase/modware-annotation/internal/message"
	"github.com/dictyBase/modware-annotation/internal/message/nats"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/dictyBase/modware-annotation/internal/repository/arangodb"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	gnats "github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const errCode = 2
const waitTime = 2

type serverParams struct {
	repo repository.TaggedAnnotationRepository
	msg  message.Publisher
}

func RunServer(clt *cli.Context) error {
	spn, err := repoAndNatsConn(clt)
	if err != nil {
		return cli.NewExitError(err.Error(), errCode)
	}
	grpcS := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_logrus.UnaryServerInterceptor(getLogger(clt)),
		),
	)
	srv, err := service.NewAnnotationService(
		&service.Params{
			Repository: spn.repo,
			Publisher:  spn.msg,
			Group:      "groups",
			Options:    getGrpcOpt(),
		})
	if err != nil {
		return cli.NewExitError(err.Error(), errCode)
	}
	annotation.RegisterTaggedAnnotationServiceServer(grpcS, srv)
	reflection.Register(grpcS)
	// create listener
	endP := fmt.Sprintf(":%s", clt.String("port"))
	lis, err := net.Listen("tcp", endP)
	if err != nil {
		return cli.NewExitError(
			fmt.Sprintf("failed to listen %s", err), errCode,
		)
	}
	log.Printf("starting grpc server on %s", endP)
	if err := grpcS.Serve(lis); err != nil {
		return cli.NewExitError(err.Error(), errCode)
	}

	return nil
}

func getLogger(clt *cli.Context) *logrus.Entry {
	log := logrus.New()
	log.Out = os.Stderr
	switch clt.GlobalString("log-format") {
	case "text":
		log.Formatter = &logrus.TextFormatter{
			TimestampFormat: "02/Jan/2006:15:04:05",
		}
	case "json":
		log.Formatter = &logrus.JSONFormatter{
			TimestampFormat: "02/Jan/2006:15:04:05",
		}
	}
	l := clt.GlobalString("log-level")
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

func allParams(
	clt *cli.Context,
) (*manager.ConnectParams, *arangodb.CollectionParams, *ontoarango.CollectionParams) {
	arPort, _ := strconv.Atoi(clt.String("arangodb-port"))

	return &manager.ConnectParams{
			User:     clt.String("arangodb-user"),
			Pass:     clt.String("arangodb-pass"),
			Database: clt.String("arangodb-database"),
			Host:     clt.String("arangodb-host"),
			Port:     arPort,
			Istls:    clt.Bool("is-secure"),
		}, &arangodb.CollectionParams{
			Annotation:   clt.String("anno-collection"),
			AnnoTerm:     clt.String("annoterm-collection"),
			AnnoVersion:  clt.String("annover-collection"),
			AnnoTagGraph: clt.String("annoterm-graph"),
			AnnoVerGraph: clt.String("annover-graph"),
			AnnoGroup:    clt.String("annogroup-collection"),
			AnnoIndexes:  clt.StringSlice("annotation-index-fields"),
		}, &ontoarango.CollectionParams{
			GraphInfo:    clt.String("cv-collection"),
			OboGraph:     clt.String("obograph"),
			Relationship: clt.String("rel-collection"),
			Term:         clt.String("term-collection"),
		}
}

func getGrpcOpt() []aphgrpc.Option {
	return []aphgrpc.Option{
		aphgrpc.TopicsOption(map[string]string{
			"annotationCreate": "AnnotationService.Create",
			"annotationDelete": "AnnotationService.Delete",
			"annotationUpdate": "AnnotationService.Update",
		}),
	}
}

func repoAndNatsConn(clt *cli.Context) (*serverParams, error) {
	anrepo, err := arangodb.NewTaggedAnnotationRepo(allParams(clt))
	if err != nil {
		return &serverParams{},
			fmt.Errorf(
				"cannot connect to arangodb annotation repository %s",
				err,
			)
	}
	msp, err := nats.NewPublisher(
		clt.String("nats-host"), clt.String("nats-port"),
		gnats.MaxReconnects(-1), gnats.ReconnectWait(waitTime*time.Second),
	)
	if err != nil {
		return &serverParams{},
			fmt.Errorf("cannot connect to messaging server %s", err)
	}

	return &serverParams{
		repo: anrepo,
		msg:  msp,
	}, nil
}

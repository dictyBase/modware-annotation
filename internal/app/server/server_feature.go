package server

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/dictyBase/aphgrpc"
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/app/service"
	"github.com/dictyBase/modware-annotation/internal/message"
	"github.com/dictyBase/modware-annotation/internal/message/nats"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/dictyBase/modware-annotation/internal/repository/arangodb"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	gnats "github.com/nats-io/nats.go"
	"github.com/urfave/cli"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type featureServerParams struct {
	repo repository.FeatureAnnotationRepository
	msg  message.FeatureAnnotationPublisher
}

func RunFeatureServer(clt *cli.Context) error {
	spn, err := featureRepoAndNatsConn(clt)
	if err != nil {
		return cli.NewExitError(err.Error(), errCode)
	}
	grpcS := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_logrus.UnaryServerInterceptor(getLogger(clt)),
		),
	)
	srv, err := service.NewFeatureAnnotationService(
		&service.FeatureParams{
			Repository: spn.repo,
			Publisher:  spn.msg,
			Options:    getFeatureGrpcOpt(),
		})
	if err != nil {
		return cli.NewExitError(err.Error(), errCode)
	}
	feature_annotation.RegisterFeatureAnnotationServiceServer(grpcS, srv)
	reflection.Register(grpcS)

	endP := fmt.Sprintf(":%s", clt.String("port"))
	lis, err := net.Listen("tcp", endP)
	if err != nil {
		return cli.NewExitError(
			fmt.Sprintf("failed to listen %s", err), errCode,
		)
	}
	log.Printf("starting feature annotation grpc server on %s", endP)
	if err := grpcS.Serve(lis); err != nil {
		return cli.NewExitError(err.Error(), errCode)
	}

	return nil
}

func allFeatureParams(
	clt *cli.Context,
) (*manager.ConnectParams, *arangodb.FeatureCollectionParams) {
	arPort, _ := strconv.Atoi(clt.String("arangodb-port"))

	return &manager.ConnectParams{
			User:     clt.String("arangodb-user"),
			Pass:     clt.String("arangodb-pass"),
			Database: clt.String("arangodb-database"),
			Host:     clt.String("arangodb-host"),
			Port:     arPort,
			Istls:    clt.Bool("is-secure"),
		}, &arangodb.FeatureCollectionParams{
			Feature: clt.String("feature-collection"),
			Pub:     clt.String("pub-collection"),
			Edge:    clt.String("edge-collection"),
			Graph:   clt.String("feature-graph"),
		}
}

func getFeatureGrpcOpt() []aphgrpc.Option {
	return []aphgrpc.Option{
		aphgrpc.TopicsOption(map[string]string{
			"featureAnnotationCreate": "FeatureAnnotationService.Create",
			"featureAnnotationUpdate": "FeatureAnnotationService.Update",
		}),
	}
}

func featureRepoAndNatsConn(clt *cli.Context) (*featureServerParams, error) {
	connectParams, collParams := allFeatureParams(clt)
	frepo, err := arangodb.NewFeatureAnnoRepo(connectParams, collParams)
	if err != nil {
		return &featureServerParams{},
			fmt.Errorf(
				"cannot connect to arangodb feature repository %s",
				err,
			)
	}
	msp, err := nats.NewFeatureAnnotationPublisher(
		clt.String("nats-host"), clt.String("nats-port"),
		gnats.MaxReconnects(-1), gnats.ReconnectWait(waitTime*time.Second),
	)
	if err != nil {
		return &featureServerParams{},
			fmt.Errorf("cannot connect to messaging server %s", err)
	}

	return &featureServerParams{
		repo: frepo,
		msg:  msp,
	}, nil
}

package service

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/dictyBase/arangomanager/testarango"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/repository/arangodb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/test/bufconn"
)

type MockMessage struct{}

func (msn *MockMessage) Publish(
	subject string,
	feat *feature.FeatureAnnotation,
) error {
	return nil
}

func (msn *MockMessage) Close() error {
	return nil
}

func setup(
	t *testing.T,
) (feature.FeatureAnnotationServiceClient, *require.Assertions) {
	t.Helper()
	assert := require.New(t)
	tra, err := testarango.NewTestArangoFromEnv(true)
	assert.NoError(err, "expect no error from creating an arangodb instance")
	repo, err := arangodb.NewFeatureAnnoRepo(
		arangodb.GetConnectParamsFromDB(tra),
		&arangodb.FeatureCollectionParams{
			Feature: "feature_test",
		},
	)
	assert.NoErrorf(
		err,
		"expect no error connecting to annotation repository, received %s",
		err,
	)
	// Create service with mock dependencies
	svc, err := NewFeatureAnnotationService(&FeatureParams{
		Repository: repo,
		Publisher:  &MockMessage{},
	})
	assert.NoError(err)

	// GRPC server setup
	server := grpc.NewServer()
	feature.RegisterFeatureAnnotationServiceServer(server, svc)
	lis := bufconn.Listen(1024 * 1024)
	go func() {
		if err := server.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
			os.Exit(1)
		}
	}()
	dialer := func(context.Context, string) (net.Conn, error) {
		conn, err := lis.Dial()
		assert.NoError(err, "expect no error from creating listener")

		return conn, nil
	}
	resolver.SetDefaultScheme("passthrough")
	// GRPC client setup
	conn, err := grpc.NewClient(
		"bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	assert.NoError(err)
	t.Cleanup(func() {
		_ = repo.Dbh().Drop()
		conn.Close()
		lis.Close()
		server.Stop()
	})

	return feature.NewFeatureAnnotationServiceClient(conn), assert
}

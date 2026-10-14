package service

import (
	"context"
	"fmt"
	"io"
	"time"

	E "github.com/IBM/fp-go/v2/either"
	F "github.com/IBM/fp-go/v2/function"
	"github.com/dictyBase/aphgrpc"
	"github.com/dictyBase/arangomanager/query"
	"github.com/dictyBase/go-genproto/dictybaseapis/annotation"
	"github.com/dictyBase/go-genproto/dictybaseapis/api/upload"
	"github.com/dictyBase/go-obograph/storage"
	"github.com/dictyBase/modware-annotation/internal/message"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/dictyBase/modware-annotation/internal/repository/arangodb"
	"github.com/go-playground/validator/v10"
	"golang.org/x/sync/errgroup"
)

const dividerVal = 1000000

type oboStreamHandler struct {
	writer *io.PipeWriter
	stream annotation.TaggedAnnotationService_OboJSONFileUploadServer
}

// Write write the content of the stream to a writer.
func (oh *oboStreamHandler) Write() error {
	defer func() { _ = oh.writer.Close() }()
	for {
		req, err := oh.stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("error in handling stream %s", err)
		}
		if _, err := oh.writer.Write(req.Content); err != nil {
			return fmt.Errorf(
				"error in writing the content from request %s",
				err,
			)
		}
	}

	return nil
}

// AnnotationService is the container for managing annotation service
// definition.
type AnnotationService struct {
	*aphgrpc.Service
	repo      repository.TaggedAnnotationRepository
	publisher message.Publisher
	group     string
	annotation.UnimplementedTaggedAnnotationServiceServer
}

// Params are the attributes that are required for creating a new AnnotationService.
type Params struct {
	Repository repository.TaggedAnnotationRepository `validate:"required"`
	Publisher  message.Publisher                     `validate:"required"`
	Options    []aphgrpc.Option                      `validate:"required"`
	Group      string                                `validate:"required"`
}

// serviceBuildState carries the validated parameters and the derived service
// container through the constructor pipeline.
type serviceBuildState struct {
	params  *Params
	options *aphgrpc.ServiceOptions
	handle  *aphgrpc.Service
}

// serviceBuildResult is the boundary record the constructor folds into.
type serviceBuildResult struct {
	service *AnnotationService
	err     error
}

func defaultOptions() *aphgrpc.ServiceOptions {
	return &aphgrpc.ServiceOptions{Resource: "annotations"}
}

// NewAnnotationService is the constructor for creating a new instance of AnnotationService.
// The whole construction is one Either pipeline forced once, at the boundary.
func NewAnnotationService(srvP *Params) (*AnnotationService, error) {
	build := F.Pipe4(
		E.TryCatchError(srvP, validator.New().Struct(srvP)),
		E.Map[error](newServiceBuildState),
		E.Map[error](withServiceHandle),
		E.Map[error](assembleAnnotationService),
		E.Fold(
			failedAnnotationService,
			successfulAnnotationService,
		),
	)

	return build.service, build.err
}

// newServiceBuildState validates nothing further and resolves the service
// options of the request.
func newServiceBuildState(srvP *Params) serviceBuildState {
	options := defaultOptions()
	for _, optfn := range srvP.Options {
		optfn(options)
	}

	return serviceBuildState{params: srvP, options: options}
}

// withServiceHandle assigns the resolved options onto a fresh aphgrpc service.
func withServiceHandle(st serviceBuildState) serviceBuildState {
	handle := &aphgrpc.Service{}
	aphgrpc.AssignFieldsToStructs(st.options, handle)
	st.handle = handle

	return st
}

// assembleAnnotationService builds the service from the validated parameters.
func assembleAnnotationService(st serviceBuildState) *AnnotationService {
	return &AnnotationService{
		Service:   st.handle,
		repo:      st.params.Repository,
		publisher: st.params.Publisher,
		group:     st.params.Group,
	}
}

// failedAnnotationService is the boundary projection of a failed constructor:
// an empty service and the validation fault.
func failedAnnotationService(err error) serviceBuildResult {
	return serviceBuildResult{
		service: &AnnotationService{},
		err:     fmt.Errorf("error in validating struct %s", err),
	}
}

// successfulAnnotationService is the boundary projection of a completed
// constructor.
func successfulAnnotationService(srv *AnnotationService) serviceBuildResult {
	return serviceBuildResult{service: srv}
}

// GetGroupResourceName returns the resource name used for annotation groups.
func (srv *AnnotationService) GetGroupResourceName() string {
	return srv.group
}

// OboJSONFileUpload uploads a obojson formatted file to the server.
func (srv *AnnotationService) OboJSONFileUpload(
	stream annotation.TaggedAnnotationService_OboJSONFileUploadServer,
) error {
	in, out := io.Pipe()
	grp := new(errgroup.Group)
	defer func() { _ = in.Close() }()
	oh := &oboStreamHandler{writer: out, stream: stream}
	grp.Go(oh.Write)
	info, err := srv.repo.LoadOboJSON(in)
	if err != nil {
		return aphgrpc.HandleGenericError(
			context.Background(),
			fmt.Errorf("error with loading obo %s", err),
		)
	}
	if err := grp.Wait(); err != nil {
		return aphgrpc.HandleGenericError(
			context.Background(),
			fmt.Errorf("error in waiting for the write to finish %s", err),
		)
	}

	err = stream.SendAndClose(&upload.FileUploadResponse{
		Status: uploadResponse(info),
		Msg:    "obojson file is uploaded",
	})
	if err != nil {
		return fmt.Errorf("error in closing the stream %s", err)
	}

	return nil
}

func uploadResponse(
	info *storage.UploadInformation,
) upload.FileUploadResponse_Status {
	if info.IsCreated {
		return upload.FileUploadResponse_CREATED
	}

	return upload.FileUploadResponse_UPDATED
}

// genNextCursorVal converts to epoch(https://en.wikipedia.org/wiki/Unix_time)
// in milliseconds.
func genNextCursorVal(t time.Time) int64 {
	return t.UnixNano() / dividerVal
}

func getAnnoAttributes(
	annom *model.AnnoDoc,
) *annotation.TaggedAnnotationAttributes {
	return &annotation.TaggedAnnotationAttributes{
		Value:         annom.Value,
		EditableValue: annom.EditableValue,
		CreatedBy:     annom.CreatedBy,
		CreatedAt:     aphgrpc.TimestampProto(annom.CreatedAt),
		Version:       annom.Version,
		EntryId:       annom.EnrtyID,
		Rank:          annom.Rank,
		IsObsolete:    annom.IsObsolete,
		Tag:           annom.Tag,
		Ontology:      annom.Ontology,
	}
}

func filterStrToQuery(filter string) (string, error) {
	var empty string
	if len(filter) == 0 {
		return empty, nil
	}
	p, err := query.ParseFilterString(filter)
	if err != nil {
		return empty, fmt.Errorf("error in parsing filter string")
	}
	filterMap := arangodb.FilterMap()
	q, err := query.GenQualifiedAQLFilterStatement(filterMap, p)
	if err != nil {
		return empty, fmt.Errorf("error in generating aql statement")
	}

	return q, nil
}

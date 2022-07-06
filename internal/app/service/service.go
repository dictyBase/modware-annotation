package service

import (
	"context"
	"fmt"
	"io"
	"time"

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

// Write write the content of the stream to a writer 
func (oh *oboStreamHandler) Write() error {
	defer oh.writer.Close()
	for {
		req, err := oh.stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("error in handling stream %s", err)
		}
		if _, err := oh.writer.Write(req.Content); err != nil {
			return fmt.Errorf("error in writing the content from request %s", err)
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

// ServiceParams are the attributes that are required for creating new AnnotationService.
type Params struct {
	Repository repository.TaggedAnnotationRepository `validate:"required"`
	Publisher  message.Publisher                     `validate:"required"`
	Options    []aphgrpc.Option                      `validate:"required"`
	Group      string                                `validate:"required"`
}

func defaultOptions() *aphgrpc.ServiceOptions {
	return &aphgrpc.ServiceOptions{Resource: "annotations"}
}

// NewAnnotationService is the constructor for creating a new instance of AnnotationService.
func NewAnnotationService(srvP *Params) (*AnnotationService, error) {
	if err := validator.New().Struct(srvP); err != nil {
		return &AnnotationService{}, fmt.Errorf("error in validating struct %s", err)
	}
	so := defaultOptions()
	for _, optfn := range srvP.Options {
		optfn(so)
	}
	srv := &aphgrpc.Service{}
	aphgrpc.AssignFieldsToStructs(so, srv)

	return &AnnotationService{
		Service:   srv,
		repo:      srvP.Repository,
		publisher: srvP.Publisher,
		group:     srvP.Group,
	}, nil
}

func (s *AnnotationService) GetGroupResourceName() string {
	return s.group
}

func (s *AnnotationService) OboJSONFileUpload(stream annotation.TaggedAnnotationService_OboJSONFileUploadServer) error {
	in, out := io.Pipe()
	grp := new(errgroup.Group)
	defer in.Close()
	oh := &oboStreamHandler{writer: out, stream: stream}
	grp.Go(oh.Write)
	info, err := s.repo.LoadOboJSON(in)
	if err != nil {
		return aphgrpc.HandleGenericError(context.Background(), fmt.Errorf("error with loading obo %s", err))
	}
	if err := grp.Wait(); err != nil {
		return aphgrpc.HandleGenericError(context.Background(), fmt.Errorf("error in waiting for the write to finish %s", err))
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

func uploadResponse(info *storage.UploadInformation) upload.FileUploadResponse_Status {
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

func getAnnoAttributes(annom *model.AnnoDoc) *annotation.TaggedAnnotationAttributes {
	return &annotation.TaggedAnnotationAttributes{
		Value:         annom.Value,
		EditableValue: annom.EditableValue,
		CreatedBy:     annom.CreatedBy,
		CreatedAt:     aphgrpc.TimestampProto(annom.CreatedAt),
		Version:       annom.Version,
		EntryId:       annom.EnrtyId,
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
	q, err := query.GenQualifiedAQLFilterStatement(arangodb.FilterMap(), p)
	if err != nil {
		return empty, fmt.Errorf("error in generating aql statement")
	}

	return q, nil
}

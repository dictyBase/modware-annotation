package arangodb

import (
	"fmt"
	"time"

	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/collection"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func validateParams(collP *FeatureCollectionParams) error {
	if err := validator.New().Struct(collP); err != nil {
		return fmt.Errorf("invalid collection parameters: %w", err)
	}

	return nil
}

func createSession(
	connP *manager.ConnectParams,
) (*manager.Session, *manager.Database, error) {
	sess, dbh, err := manager.NewSessionDb(connP)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to create database session: %w",
			err,
		)
	}

	return sess, dbh, nil
}

func createFeatureCollection(
	dbh *manager.Database,
	collP *FeatureCollectionParams,
) (driver.Collection, error) {
	schema, err := model.FeatureAnnotationSchema()
	if err != nil {
		return nil, fmt.Errorf(
			"failed to generate feature annotation schema: %w",
			err,
		)
	}

	schemaOpt := &driver.CollectionSchemaOptions{
		Level:   driver.CollectionSchemaLevelModerate,
		Message: "Feature annotation validation failed",
		Type:    "json",
	}
	if err := schemaOpt.LoadRule(schema); err != nil {
		return nil, fmt.Errorf("error in loading schema %s", err)
	}
	coll, err := dbh.FindOrCreateCollection(
		collP.Feature,
		&driver.CreateCollectionOptions{
			Schema: schemaOpt,
		},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create/find feature collection: %w",
			err,
		)
	}

	return coll, nil
}

func createIndices(dbh *manager.Database, coll driver.Collection) error {
	_, _, err := dbh.EnsurePersistentIndex(
		coll.Name(),
		[]string{"feature_id"},
		&driver.EnsurePersistentIndexOptions{
			InBackground: true,
			Unique:       true,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create id-version index: %w", err)
	}

	_, _, err = dbh.EnsurePersistentIndex(
		coll.Name(),
		[]string{"name"},
		&driver.EnsurePersistentIndexOptions{
			InBackground: true,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to create name index: %w", err)
	}

	return nil
}

func updateBasicFields(
	faDoc *model.FeatureAnnotationDoc,
	doc *feature.FeatureAnnotationUpdate,
) {
	faDoc.UpdatedBy = doc.UpdatedBy
	if faDoc.IsObsolete != doc.IsObsolete {
		faDoc.IsObsolete = doc.IsObsolete
	}
}

func updateAttributes(
	mdoc *model.FeatureAnnotationDoc,
	attrs *feature.FeatureAnnotationAttributes,
) {
	mdoc.Name = attrs.Name
	mdoc.Synonyms = append(mdoc.Synonyms, attrs.Synonyms...)
	mdoc.Publications = append(mdoc.Publications, attrs.Publications...)
	mdoc.Pubmed = append(mdoc.Pubmed, attrs.Pubmed...)
	mdoc.DbLinks = append(
		mdoc.DbLinks,
		collection.Map(attrs.Dblinks, convertDbLink)...)
	mdoc.Properties = append(
		mdoc.Properties,
		collection.Map(attrs.Properties, convertProperty)...)
}

func convertDbLink(link *feature.DbLink) model.DbLinkDoc {
	return model.DbLinkDoc{
		PrimaryId: link.PrimaryId,
		Database:  link.Database,
		Version:   link.Version,
		LinkType:  link.Linktype,
		URL:       link.Url,
		Label:     link.Label,
	}
}

func convertProperty(prop *feature.TagProperty) model.TagPropertyDoc {
	mprop := model.TagPropertyDoc{
		Tag:       prop.Tag,
		Value:     prop.Value,
		CreatedBy: prop.CreatedBy,
		UpdatedBy: prop.CreatedBy,
		CreatedAt: prop.CreatedAt.AsTime(),
		UpdatedAt: prop.CreatedAt.AsTime(),
	}
	if len(prop.UpdatedBy) != 0 {
		mprop.UpdatedBy = prop.UpdatedBy
	}
	if prop.UpdatedAt != nil {
		mprop.UpdatedAt = prop.UpdatedAt.AsTime()
	}

	return mprop
}

func setOptionalFields(
	doc *feature.NewFeatureAnnotation,
	faDoc *model.FeatureAnnotationDoc,
) *model.FeatureAnnotationDoc {
	faDoc.Synonyms = doc.Attributes.Synonyms
	faDoc.Publications = doc.Attributes.Publications
	faDoc.Pubmed = doc.Attributes.Pubmed
	faDoc.DbLinks = collection.Map(doc.Attributes.Dblinks, convertDbLink)
	faDoc.Properties = collection.Map(
		doc.Attributes.Properties,
		convertProperty,
	)
	return faDoc
}

func verifyRemoval(
	identifier string,
	repo repository.FeatureAnnotationRepository,
	assert *require.Assertions,
) {
	_, err := repo.GetFeatureAnnotation(identifier)
	assert.Error(err, "expected error getting removed feature annotation")
	assert.True(
		repository.IsAnnotationNotFound(err),
		"should be annotation not found error",
	)
}

func getTestIdentifier(
	wantErr bool,
	repo repository.FeatureAnnotationRepository,
	assert *require.Assertions,
) string {
	if wantErr {
		return "non_existent_id"
	}
	baseDoc := &feature.NewFeatureAnnotation{
		Id:        "DDB_G0285425",
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
		Attributes: &feature.FeatureAnnotationAttributes{
			Name: "test gene",
		},
	}
	doc, err := repo.AddFeatureAnnotation(baseDoc)
	assert.NoError(err, "expected no error adding test feature annotation")

	return doc.AnnoId
}

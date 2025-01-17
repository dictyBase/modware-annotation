package arangodb

import (
	"fmt"

	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/collection"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/go-playground/validator/v10"
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
		[]string{"id"},
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
	if len(attrs.Name) > 0 {
		mdoc.Name = attrs.Name
	}
	if len(attrs.Synonyms) > 0 {
		mdoc.Synonyms = append(mdoc.Synonyms, attrs.Synonyms...)
	}
	if len(attrs.Publications) > 0 {
		mdoc.Publications = append(mdoc.Publications, attrs.Publications...)
	}
	if len(attrs.Pubmed) > 0 {
		mdoc.Pubmed = append(mdoc.Pubmed, attrs.Pubmed...)
	}
	if len(attrs.Dblinks) > 0 {
		mdoc.DbLinks = append(
			mdoc.DbLinks,
			collection.Map(attrs.Dblinks, convertDbLink)...)
	}
	if len(attrs.Properties) > 0 {
		mdoc.Properties = append(
			mdoc.Properties,
			collection.Map(attrs.Properties, convertProperty)...)
	}
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
	return model.TagPropertyDoc{
		Tag:   prop.Tag,
		Value: prop.Value,
	}
}

func setOptionalFields(
	doc *feature.NewFeatureAnnotation,
	faDoc *model.FeatureAnnotationDoc,
) {
	if len(doc.Attributes.Synonyms) > 0 {
		faDoc.Synonyms = doc.Attributes.Synonyms
	}
	if len(doc.Attributes.Publications) > 0 {
		faDoc.Publications = doc.Attributes.Publications
	}
	if len(doc.Attributes.Pubmed) > 0 {
		faDoc.Pubmed = doc.Attributes.Pubmed
	}
	if len(doc.Attributes.Dblinks) > 0 {
		faDoc.DbLinks = collection.Map(doc.Attributes.Dblinks, toDbLink)
	}
	if len(doc.Attributes.Properties) > 0 {
		faDoc.Properties = collection.Map(
			doc.Attributes.Properties,
			func(prop *feature.TagProperty) model.TagPropertyDoc {
				return model.TagPropertyDoc{
					Tag:   prop.Tag,
					Value: prop.Value,
				}
			},
		)
	}
}

func toDbLink(link *feature.DbLink) model.DbLinkDoc {
	dbLink := model.DbLinkDoc{
		PrimaryId: link.PrimaryId,
		Version:   link.Version,
		Database:  link.Database,
	}

	if link.Linktype != "" {
		dbLink.LinkType = link.Linktype
	}
	if link.Url != "" {
		dbLink.URL = link.Url
	}
	if link.Label != "" {
		dbLink.Label = link.Label
	}

	return dbLink
}

package arangodb

import (
	"fmt"

	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/collection"
	"github.com/dictyBase/modware-annotation/internal/model"
)

// CreateIndexArgs holds the arguments for the createIndices function.
type CreateIndexArgs struct {
	Dbh          *manager.Database
	Coll         driver.Collection
	Fields       []string
	UniqueFields []string
	ErrPrefix    string
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

// createIndices creates persistent indices for a collection based on the provided arguments.
func createIndices(args *CreateIndexArgs) error {
	// Create unique indices
	for _, field := range args.UniqueFields {
		_, _, err := args.Dbh.EnsurePersistentIndex(
			args.Coll.Name(),
			[]string{field},
			&driver.EnsurePersistentIndexOptions{
				InBackground: true,
				Unique:       true,
			},
		)
		if err != nil {
			return fmt.Errorf(
				"failed to create unique %s index for %s: %w",
				field,
				args.ErrPrefix,
				err,
			)
		}
	}

	// Create non-unique indices
	for _, field := range args.Fields {
		_, _, err := args.Dbh.EnsurePersistentIndex(
			args.Coll.Name(),
			[]string{field},
			&driver.EnsurePersistentIndexOptions{
				InBackground: true,
			},
		)
		if err != nil {
			return fmt.Errorf(
				"failed to create %s index for %s: %w",
				field,
				args.ErrPrefix,
				err,
			)
		}
	}

	return nil
}

func createFeatureIndices(dbh *manager.Database, coll driver.Collection) error {
	return createIndices(&CreateIndexArgs{
		Dbh:          dbh,
		Coll:         coll,
		Fields:       []string{"name"},
		UniqueFields: []string{"feature_id"},
		ErrPrefix:    "feature collection",
	})
}

func createPubIndices(dbh *manager.Database, coll driver.Collection) error {
	return createIndices(&CreateIndexArgs{
		Dbh:          dbh,
		Coll:         coll,
		UniqueFields: []string{"id"},
		ErrPrefix:    "pub collection",
	})
}

func createPubCollection(
	dbh *manager.Database,
	collP *FeatureCollectionParams,
) (driver.Collection, error) {
	schema, err := model.PubSchema()
	if err != nil {
		return nil, fmt.Errorf(
			"failed to generate pub schema: %w",
			err,
		)
	}

	schemaOpt := &driver.CollectionSchemaOptions{
		Level:   driver.CollectionSchemaLevelModerate,
		Message: "Pub validation failed",
		Type:    "json",
	}
	if err := schemaOpt.LoadRule(schema); err != nil {
		return nil, fmt.Errorf("error in loading pub schema %s", err)
	}
	coll, err := dbh.FindOrCreateCollection(
		collP.Pub,
		&driver.CreateCollectionOptions{
			Schema: schemaOpt,
		},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create/find pub collection: %w",
			err,
		)
	}

	return coll, nil
}

func createEdgeCollection(
	dbh *manager.Database,
	collP *FeatureCollectionParams,
) (driver.Collection, error) {
	// Assuming edge collection doesn't need a schema for now
	coll, err := dbh.FindOrCreateCollection(
		collP.Edge,
		&driver.CreateCollectionOptions{
			Type: driver.CollectionTypeEdge,
		},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create/find edge collection: %w",
			err,
		)
	}

	return coll, nil
}

func createFeaturePubGraph(
	dbh *manager.Database,
	graphName string,
	featureColl driver.Collection,
	pubColl driver.Collection,
	edgeColl driver.Collection,
) (driver.Graph, error) {
	grph, err := dbh.FindOrCreateGraph(
		graphName,
		[]driver.EdgeDefinition{
			{
				Collection: edgeColl.Name(),
				From:       []string{featureColl.Name()},
				To:         []string{pubColl.Name()},
			},
		})
	if err != nil {
		return grph, fmt.Errorf(
			"failed to create/find graph %s: %w",
			graphName,
			err,
		)
	}

	return grph, nil
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

package arangodb

import (
	"fmt"
	"time"

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
	if err = schemaOpt.LoadRule(schema); err != nil {
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
	if err = schemaOpt.LoadRule(schema); err != nil {
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

// copyFeatureAnnotationDoc creates a deep copy of a FeatureAnnotationDoc.
func copyFeatureAnnotationDoc(
	doc *model.FeatureAnnotationDoc,
) *model.FeatureAnnotationDoc {
	if doc == nil {
		return nil
	}

	// Create a new instance with all fields copied
	newDoc := &model.FeatureAnnotationDoc{
		DocumentMeta: doc.DocumentMeta,
		Type:         doc.Type,
		AnnoID:       doc.AnnoID,
		CreatedAt:    doc.CreatedAt,
		UpdatedAt:    doc.UpdatedAt,
		CreatedBy:    doc.CreatedBy,
		UpdatedBy:    doc.UpdatedBy,
		Name:         doc.Name,
		IsObsolete:   doc.IsObsolete,
		NotFound:     doc.NotFound,
	}

	// Deep copy slices - only if they have content
	if len(doc.Synonyms) > 0 {
		newDoc.Synonyms = make([]string, len(doc.Synonyms))
		copy(newDoc.Synonyms, doc.Synonyms)
	}

	if len(doc.Publications) > 0 {
		newDoc.Publications = make([]string, len(doc.Publications))
		copy(newDoc.Publications, doc.Publications)
	}

	if len(doc.Pubmed) > 0 {
		newDoc.Pubmed = make([]string, len(doc.Pubmed))
		copy(newDoc.Pubmed, doc.Pubmed)
	}

	// Deep copy DbLinks
	if len(doc.DBLinks) > 0 {
		newDoc.DBLinks = make([]model.DBLinkDoc, len(doc.DBLinks))
		copy(newDoc.DBLinks, doc.DBLinks)
	}

	// Deep copy Properties
	if len(doc.Properties) > 0 {
		newDoc.Properties = make([]model.TagPropertyDoc, len(doc.Properties))
		copy(newDoc.Properties, doc.Properties)
	}

	return newDoc
}

func updateBasicFields(
	faDoc *model.FeatureAnnotationDoc,
	doc *feature.FeatureAnnotationUpdate,
) *model.FeatureAnnotationDoc {
	// Create a copy of the document
	newDoc := copyFeatureAnnotationDoc(faDoc)

	// Update fields on the copy
	newDoc.UpdatedBy = doc.UpdatedBy
	if newDoc.IsObsolete != doc.IsObsolete {
		newDoc.IsObsolete = doc.IsObsolete
	}

	return newDoc
}

func updateAttributes(
	mdoc *model.FeatureAnnotationDoc,
	attrs *feature.FeatureAnnotationAttributes,
) *model.FeatureAnnotationDoc {
	// Create a copy of the document
	newDoc := copyFeatureAnnotationDoc(mdoc)

	// Update fields on the copy
	newDoc.Name = attrs.Name
	// Append synonyms to existing ones (original behavior)
	newDoc.Synonyms = append(newDoc.Synonyms, attrs.Synonyms...)
	// Append dblinks to existing ones (original behavior)
	newDoc.DBLinks = append(
		newDoc.DBLinks,
		collection.Map(attrs.Dblinks, convertDBLink)...)
	// Replace properties (original behavior)
	newDoc.Properties = collection.Map(attrs.Properties, convertProperty)

	return newDoc
}

// updateAttributesPartial handles partial updates from
// FeatureAnnotationUpdateAttributes. Only non-empty/non-nil fields are updated,
// preserving existing values otherwise.
func updateAttributesPartial(
	mdoc *model.FeatureAnnotationDoc,
	attrs *feature.FeatureAnnotationUpdateAttributes,
) *model.FeatureAnnotationDoc {
	// Create a copy of the document
	newDoc := copyFeatureAnnotationDoc(mdoc)

	// Update name only if provided
	if len(attrs.Name) > 0 {
		newDoc.Name = attrs.Name
	}

	// Update synonyms only if provided (replaces existing)
	if len(attrs.Synonyms) > 0 {
		newDoc.Synonyms = append([]string(nil), attrs.Synonyms...)
	}

	// Update dblinks only if provided (replaces existing)
	if len(attrs.Dblinks) > 0 {
		newDoc.DBLinks = collection.Map(attrs.Dblinks, convertDBLink)
	}

	// Update properties only if provided (replaces existing, consistent with SetTags)
	if len(attrs.Properties) > 0 {
		newDoc.Properties = collection.Map(attrs.Properties, convertProperty)
	}

	return newDoc
}

func convertDBLink(link *feature.DbLink) model.DBLinkDoc {
	return model.DBLinkDoc{
		PrimaryID: link.PrimaryId,
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

func convertNewTagToModel(tag *feature.TagPropertyCreate) model.TagPropertyDoc {
	createdAt := time.Now()
	if tag.CreatedAt.IsValid() {
		createdAt = tag.CreatedAt.AsTime()
	}
	return model.TagPropertyDoc{
		Tag:       tag.Tag,
		Value:     tag.Value,
		CreatedBy: tag.CreatedBy,
		CreatedAt: createdAt,
		UpdatedBy: tag.CreatedBy,
		UpdatedAt: createdAt,
	}
}

func setOptionalFields(
	doc *feature.NewFeatureAnnotation,
	faDoc *model.FeatureAnnotationDoc,
) *model.FeatureAnnotationDoc {
	faDoc.Synonyms = doc.Attributes.Synonyms
	faDoc.DBLinks = collection.Map(doc.Attributes.Dblinks, convertDBLink)
	faDoc.Properties = collection.Map(
		doc.Attributes.Properties,
		convertProperty,
	)

	return faDoc
}

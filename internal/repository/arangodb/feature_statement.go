package arangodb

const (
	// featurePubEdgeQ defines an AQL query to insert an edge between a feature and a publication.
	// It connects documents from the feature collection to the publication collection
	// and includes a 'source' attribute on the edge.
	featurePubEdgeQ = `
		INSERT {
			_from: @feature_key,
			_to: @pub_key,
			source: @source
		} IN @@edge_collection
	`

	// pubUpsertQ defines an UPSERT AQL query for the publication
	// collection. It inserts a new publication document with id,
	// created_at, and updated_at if it doesn't exist, or updates the
	// updated_at field if it does.
	pubUpsertQ = `
		UPSERT { id: @id }
		INSERT { 
			id: @id, 
			created_at: DATE_ISO8601(DATE_NOW()), 
			updated_at: DATE_ISO8601(DATE_NOW()) 
		}
		UPDATE { updated_at: DATE_ISO8601(DATE_NOW()) }
		IN @@collection
		RETURN NEW
	`

	featureExistQ = `
		FOR f IN @@collection
			FILTER f.feature_id == @id
			LIMIT 1
			RETURN f._key
	`
	featurePurgeQ = `
        FOR f IN @@collection
            FILTER f.feature_id == @id
            LIMIT 1
            REMOVE f IN @@collection
    `
	featureObsoleteQ = `
        FOR f IN @@collection
            FILTER f.feature_id == @id
            UPDATE f WITH { is_obsolete: true } IN @@collection
    `
	featureGetByIdQ = `FOR f IN @@collection 
    		FILTER f.feature_id == @id 
    		FILTER f.is_obsolete == false 
    		LIMIT 1 
    		RETURN f`
)

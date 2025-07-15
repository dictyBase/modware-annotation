package arangodb

const (
	// featurePubEdgeQ defines an AQL query to insert edges between a
	// feature and multiple publications. It iterates over a list of
	// publication keys (@pub_keys) and inserts an edge connecting the
	// feature (@feature_key) to each publication (pub_key) in the specified
	// edge collection (@@edge_collection), including a 'source' attribute.
	featurePubEdgeQ = `
		FOR pub_key IN @pub_keys
			INSERT {
				_from: @feature_key,
				_to: pub_key,
				source: @source
			} IN @@edge_collection
	`

	// pubUpsertQ defines an UPSERT AQL query for the publication
	// collection. It iterates over a list of publication IDs (@ids). For each ID,
	// it inserts a new publication document with id, created_at, and updated_at
	// if it doesn't exist, or updates the updated_at field if it does.
	// It returns the newly created or updated documents.
	pubUpsertQ = `
		LET allKeys = (
			FOR id_val IN @ids
				UPSERT { id: id_val }
				INSERT {
					id: id_val,
					created_at: DATE_ISO8601(DATE_NOW()),
					updated_at: DATE_ISO8601(DATE_NOW())
				}
				UPDATE { updated_at: DATE_ISO8601(DATE_NOW()) }
				IN @@collection
				RETURN NEW._id
		)
		RETURN allKeys
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
	featureGetByIdQ = `
	FOR f IN @@collection 
	    FILTER f.feature_id == @id 
	    FILTER f.is_obsolete == false 
	    LIMIT 1
	    LET pubmed = (
		FOR v,e IN 1..1 OUTBOUND f GRAPH @graph 
		FILTER e.source == 'pubmed'
		RETURN v.id	
	    )
	    LET doi = (
		FOR v,e IN 1..1 OUTBOUND f GRAPH @graph 
		FILTER e.source == 'doi'
		RETURN v.id	
	    )
	    RETURN MERGE(f, {pubmed: pubmed, publications: doi})
    `

	featAnnoGetByNameQ = `
	FOR f IN @@collection 
	    FILTER f.name == @name 
	    FILTER f.is_obsolete == false 
	    LIMIT 1
	    LET pubmed = (
		FOR v,e IN 1..1 OUTBOUND f GRAPH @graph 
		FILTER e.source == 'pubmed'
		RETURN v.id	
	    )
	    LET doi = (
		FOR v,e IN 1..1 OUTBOUND f GRAPH @graph 
		FILTER e.source == 'doi'
		RETURN v.id	
	    )
	    RETURN MERGE(f, {pubmed: pubmed, publications: doi})
	`

	featureByPublicationIdQ = `
	FOR pub IN @@collection
    		FOR v,e IN 1..1 INBOUND pub GRAPH @graph
        	FILTER pub.id == @id
        	FILTER e.source == @source
        	FILTER v.is_obsolete == false
        	RETURN v
   	`

	featurePropsAppendQ = `
	FOR doc IN @@collection
		FILTER doc._key == @key
		UPDATE doc WITH {
			properties: APPEND(doc.properties, @newprops)
		} IN @@collection 
		RETURN NEW
	`
)

package arangodb

const (
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

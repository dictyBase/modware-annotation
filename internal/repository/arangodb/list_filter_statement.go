package arangodb

const (
	cvtExclusiveListFilterQ = `
		LET cvtlist = (
		    FOR cvt IN @@cvterm_collection
			FOR cv IN @@cv_collection
		        	FILTER cvt.graph_id == cv._id
				FILTER cvt.deprecated == false
				%s
				RETURN { cv: cv, cvterm: cvt }
		)
		
		FOR row IN cvtlist
		    FOR entry IN 1..1 INBOUND row.cvterm GRAPH @anno_cvterm_graph
			    FILTER entry.is_obsolete == false
		            LIMIT @limit
		            RETURN MERGE(entry, { 
				tag: row.cvterm.label,
				ontology: row.cv.metadata.namespace
			    })
	`

	annExclusiveListFilterQ = `
		LET annentries = (
		    FOR ann IN @@anno_collection
			%s
		        FILTER ann.is_obsolete == false
			SORT ann.created_at DESC
		        RETURN ann
		)
		
		FOR entry IN annentries
		    FOR cvt IN 1..1 OUTBOUND entry GRAPH @anno_cvterm_graph
		        FOR cv IN @@cv_collection
		            FILTER cvt.graph_id == cv._id
			    FILTER cvt.deprecated == false
		            LIMIT @limit
		            RETURN MERGE(entry, { 
				tag: cvt.label, 
				ontology: cv.metadata.namespace
			    })
	`
	annCvtListFilterQ = `
		LET annentries = (
		    FOR ann IN @@anno_collection
			%s
		        FILTER ann.is_obsolete == false
			SORT ann.created_at DESC
		        RETURN ann
		)
		
		FOR entry IN annentries
		    FOR cvt IN 1..1 OUTBOUND entry GRAPH @anno_cvterm_graph
		        FOR cv IN @@cv_collection
		            FILTER cvt.graph_id == cv._id
			    FILTER cvt.deprecated == false
			    %s
		            LIMIT @limit
		            RETURN MERGE(entry, { 
				tag: cvt.label, 
				ontology: cv.metadata.namespace
			    })
	`

	cvtExclusiveListFilterWithCursorQ = `
		LET cvtlist = (
		    FOR cvt IN @@cvterm_collection
			FOR cv IN @@cv_collection
		        	FILTER cvt.graph_id == cv._id
				FILTER cvt.deprecated == false
				%s
				RETURN { cv: cv, cvterm: cvt }
		)
		
		FOR row IN cvtlist
		    FOR entry IN 1..1 INBOUND row.cvterm GRAPH @anno_cvterm_graph
			    FILTER entry.is_obsolete == false
			    FILTER entry.created_at <= DATE_ISO8601(@cursor)
			    SORT entry.created_at DESC
		            LIMIT @limit
		            RETURN MERGE(entry, { 
				tag: row.cvterm.label,
				ontology: row.cv.metadata.namespace
			    })
	`

	annExclusiveListFilterWithCursorQ = `
		LET annentries = (
		    FOR ann IN @@anno_collection
			%s
		        FILTER ann.is_obsolete == false
			FILTER ann.created_at <= DATE_ISO8601(@cursor)
			SORT ann.created_at DESC
		        RETURN ann
		)
		
		FOR entry IN annentries
		    FOR cvt IN 1..1 OUTBOUND entry GRAPH @anno_cvterm_graph
		        FOR cv IN @@cv_collection
		            FILTER cvt.graph_id == cv._id
			    FILTER cvt.deprecated == false
		            LIMIT @limit
		            RETURN MERGE(entry, { 
				tag: cvt.label, 
				ontology: cv.metadata.namespace
			    })
	`

	annCvtListFilterWithCursorQ = `
		LET annentries = (
		    FOR ann IN @@anno_collection
			%s
		        FILTER ann.is_obsolete == false
			FILTER ann.created_at <= DATE_ISO8601(@cursor)
			SORT ann.created_at DESC
		        RETURN ann
		)
		
		FOR entry IN annentries
		    FOR cvt IN 1..1 OUTBOUND entry GRAPH @anno_cvterm_graph
		        FOR cv IN @@cv_collection
		            FILTER cvt.graph_id == cv._id
			    FILTER cvt.deprecated == false
			    %s
		            LIMIT @limit
		            RETURN MERGE(entry, { 
				tag: cvt.label, 
				ontology: cv.metadata.namespace
			    })
	`
)

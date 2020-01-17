package arangodb

const (
	cvtID2LblQ = `
		FOR cvt IN @@cvterm_collection
			FILTER cvt._id == @id
			RETURN cvt.label
	`
	annExistTagQ = `
		FOR cv IN @@cv_collection
			FOR cvt IN @@cvterm_collection
				FILTER cv.metadata.namespace == @ontology
				FILTER cvt.label == @tag || @tag IN cvt.metadata.synonyms[*].value
				FILTER cvt.graph_id == cv._id
				FILTER cvt.deprecated == false
				RETURN cvt._id
	`
	annExistQ = `
		FOR ann IN @@anno_collection
			FOR v IN 1..1 OUTBOUND ann GRAPH @anno_cvterm_graph
				FOR cv IN @@cv_collection
					FILTER ann.entry_id == @entry_id
					FILTER ann.rank == @rank
					FILTER ann.is_obsolete == false
					FILTER v.label == @tag
					FILTER v.deprecated == false
					FILTER v.graph_id == cv._id
					FILTER cv.metadata.namespace == @ontology
					RETURN ann
	`
	annInst = `
		LET n = (
			INSERT {
					value: @value,
					editable_value: @editable_value,
					created_by: @created_by,
					entry_id: @entry_id,
					rank: @rank,
					is_obsolete: false,
					version: @version,
					created_at: DATE_ISO8601(DATE_NOW())
				   } IN @@anno_collection RETURN NEW
		)
		INSERT { _from: n[0]._id, _to: @to } IN @@anno_cv_collection
		RETURN n[0]
	`
	annListQ = `
		FOR ann IN @@anno_collection
			FOR cvt IN 1..1 OUTBOUND ann GRAPH @anno_cvterm_graph
				FOR cv IN @@cv_collection
					FILTER ann.is_obsolete == false
					FILTER cvt.graph_id == cv._id
					SORT ann.created_at DESC
					LIMIT @limit
						RETURN MERGE(
							ann,
							{ tag: cvt.label, 
							  ontology: cv.metadata.namespace 
							})
	`
	annListFilterQ = `
		FOR ann IN @@anno_collection
			FOR cvt IN 1..1 OUTBOUND ann GRAPH @anno_cvterm_graph
				FOR cv IN @@cv_collection
					FILTER ann.is_obsolete == false
					FILTER cvt.graph_id == cv._id
					%s
					SORT ann.created_at DESC
					LIMIT @limit
						RETURN MERGE(
							ann,
							{ tag: cvt.label, 
							  ontology: cv.metadata.namespace 
							})
	`
	annListWithCursorQ = `
		FOR ann IN @@anno_collection
			FOR cvt IN 1..1 OUTBOUND ann GRAPH @anno_cvterm_graph
				FOR cv IN @@cv_collection
					FILTER ann.is_obsolete == false
					FILTER cvt.graph_id == cv._id
					FILTER ann.created_at <= DATE_ISO8601(@cursor)
					SORT ann.created_at DESC
					LIMIT @limit
						RETURN MERGE(
							ann,
							{ tag: cvt.label, ontology: cv.metadata.namespace }
						)
	`
	annListFilterWithCursorQ = `
		FOR ann IN @@anno_collection
			FOR cvt IN 1..1 OUTBOUND ann GRAPH @anno_cvterm_graph
				FOR cv IN @@cv_collection
					FILTER ann.is_obsolete == false
					FILTER cvt.graph_id == cv._id
					FILTER ann.created_at <= DATE_ISO8601(@cursor)
					%s
					SORT ann.created_at DESC
					LIMIT @limit
						RETURN MERGE(
							ann,
							{ tag: cvt.label, ontology: cv.metadata.namespace }
						)
	`
	annGroupInst = `
		INSERT {
				created_at: DATE_ISO8601(DATE_NOW()),
				updated_at: DATE_ISO8601(DATE_NOW()),
				group: @group
			   } IN @@anno_group_collection RETURN NEW
	`
	annGroupUpd = `
		UPDATE { _key: @key }
			WITH { 
					updated_at: DATE_ISO8601(DATE_NOW()),
					group: @group 
				 } IN @@anno_group_collection RETURN NEW
	`
	annGroupListFilterQ = `
		LET filterannos = (
			FOR ann IN %s
				FOR cvt IN 1..1 OUTBOUND ann GRAPH '%s'
					FOR cv IN %s
						FILTER ann.is_obsolete == false
						FILTER cvt.graph_id == cv._id
						%s
						RETURN ann._key
		)
		FOR ag in %s
			LET annotations = (
				FOR aid in ag.group
					FOR ann IN %s
						FOR cvt IN 1..1 OUTBOUND ann GRAPH '%s'
							FOR cv IN %s
								FILTER aid == ann._key
								FILTER cvt.graph_id == cv._id
								RETURN MERGE(
									ann,
									{ tag: cvt.label, ontology: cv.metadata.namespace }
								)
			)
			FILTER ag.group ANY IN filterannos
			SORT ag.created_at DESC
			LIMIT %d
			RETURN {
				created_at: ag.created_at,
				updated_at: ag.updated_at,
				group_id: ag._key,
				annotations: annotations
			}
	`
	annGroupListFilterWithCursorQ = `
		LET filterannos = (
			FOR ann IN %s
				FOR cvt IN 1..1 OUTBOUND ann GRAPH '%s'
					FOR cv IN %s
						FILTER ann.is_obsolete == false
						FILTER cvt.graph_id == cv._id
						%s
						RETURN ann._key
		)
		FOR ag in %s
			LET annotations = (
				FOR aid in ag.group
					FOR ann IN %s
						FOR cvt IN 1..1 OUTBOUND ann GRAPH '%s'
							FOR cv IN %s
								FILTER aid == ann._key
								FILTER cvt.graph_id == cv._id
								RETURN MERGE(
									ann,
									{ tag: cvt.label, ontology: cv.metadata.namespace }
								)
			)
			FILTER ag.group ANY IN filterannos
			FILTER ag.created_at <= DATE_ISO8601(%d)
			SORT ag.created_at DESC
			LIMIT %d
			RETURN {
				created_at: ag.created_at,
				updated_at: ag.updated_at,
				group_id: ag._key,
				annotations: annotations
			}
	`
	annGroupListQ = `
		FOR ag IN %s
			LET annotations = (
				FOR aid in ag.group
					FOR ann IN %s
						FOR cvt IN 1..1 OUTBOUND ann GRAPH '%s'
							FOR cv IN %s
								FILTER aid == ann._key
								FILTER cvt.graph_id == cv._id
								RETURN MERGE(
									ann,
									{ tag: cvt.label, ontology: cv.metadata.namespace }
								)
			)
			SORT ag.created_at DESC
			LIMIT %d
			RETURN {
				created_at: ag.created_at,
				updated_at: ag.updated_at,
				group_id: ag._key,
				annotations: annotations
			}
	`
	annGroupListWithCursorQ = `
		FOR ag IN %s
			LET annotations = (
				FOR aid in ag.group
					FOR ann IN %s
						FOR cvt IN 1..1 OUTBOUND ann GRAPH '%s'
							FOR cv IN %s
								FILTER aid == ann._key
								FILTER cvt.graph_id == cv._id
								RETURN MERGE(
									ann,
									{ tag: cvt.label, ontology: cv.metadata.namespace }
								)
			)
			FILTER ag.created_at <= DATE_ISO8601(%d)
			SORT ag.created_at DESC
			LIMIT %d
			RETURN {
				created_at: ag.created_at,
				updated_at: ag.updated_at,
				group_id: ag._key,
				annotations: annotations
			}
	`
	annVerInstFn = `
		function (params) {
			var db = require('@arangodb').db
			var d = new Date(Date.now())
			var annoc = db._collection(params[0])
			var n = annoc.save({
				value: params[3],
				editable_value: params[4],
				created_by: params[5],
				entry_id: params[6],
				rank: params[7],
				is_obsolete: false,
				version: params[8],
				created_at: d.toISOString()
			}, { returnNew: true})
			annoc.update(params[10],{ is_obsolete: true })
			db._collection(params[1]).save({
				_from: n._id,
				_to: params[9]
			})
			db._collection(params[2]).save({
				_from: params[10],
				_to: n._id
			})
			return n.new
		}
	`
	annGetQ = `
		FOR ann IN %s
			FOR v IN 1..1 OUTBOUND ann GRAPH '%s'
		        FOR cv IN %s
					FILTER ann._key == '%s'
					FILTER v.graph_id == cv._id
					LIMIT 1
					RETURN MERGE(
						ann,
						{ ontology: cv.metadata.namespace, tag: v.label, cvtid: v._id}
					)
	`
	annGetByEntryQ = `
		FOR ann IN %s
			FOR v IN 1..1 OUTBOUND ann GRAPH '%s'
				FOR cv IN %s
					FILTER ann.entry_id == '%s'
					FILTER ann.rank == %d
					FILTER ann.is_obsolete == %t
					FILTER v.label == '%s'
					FILTER v.graph_id == cv._id
					FILTER cv.metadata.namespace == '%s'
					SORT ann.version DESC
					LIMIT 1
					RETURN MERGE(ann, { ontology: cv.metadata.namespace, tag: v.label })
	`
)

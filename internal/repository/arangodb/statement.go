package arangodb

const (
	annExistTagQ = `
		FOR cv IN @@cv_collection
			FOR cvt IN @@cvterm_collection
				FILTER cv.metadata.namespace == @ontology
				FILTER cvt.label == @tag
				FILTER cvt.graph_id == cv._id
				RETURN cvt._id
	`
	annExistQ = `
		FOR ann IN %s
			FOR v IN 1..1 OUTBOUND ann GRAPH '%s'
				FOR cv IN %s
					FILTER ann.entry_id == '%s'
					FILTER ann.rank == %d
					FILTER ann.is_obsolete == false
					FILTER v.label == '%s'
					FILTER v.graph_id == cv._id
					FILTER cv.metadata.namespace == '%s'
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
			FOR cvt IN 1..1 OUTBOUND ann GRAPH '@graph'
				FOR cv IN @@cv_collection
					FILTER ann.is_obsolete == false
					FILTER cvt.graph_id == cv._id
					SORT ann.created_at DESC
					LIMIT @limit
					RETURN MERGE(
						ann,
						{ tag: cvt.label, ontology.cv.metadata.namespace }
					)
	`
	annListWithCursorQ = `
		FOR ann IN @@anno_collection
			FOR cvt IN 1..1 OUTBOUND ann GRAPH '@graph'
				FOR cv IN @@cv_collection
					FILTER ann.is_obsolete == false
					FILTER cvt.graph_id == cv._id
					FILTER ann.created_at <= DATE_ISO8601(@next_cursor)
					SORT ann.created_at DESC
					LIMIT @limit
					RETURN MERGE(
						ann,
						{ tag: cvt.label, ontology.cv.metadata.namespace }
					)
	`
	annVerInst = `
		LET n = (
			INSERT {
					value: @value,
					editable_value: @editable_value,
					created_by: @created_by,
					entry_id: @entry_id,
					rank: @rank,
					is_obsolete: false,
					version: @version,
					created_at: DATE_ISO8601(DATE_NOW()),
				   } IN @@anno_collection RETURN NEW
		)
		INSERT { _from: n[0]._id, _to: @to } IN @@anno_cv_collection
		INSERT { _from: @prev, _to: n[0]._id } IN @anno_ver_collection
		RETURN n[0]
	`
	annGetQ = `
		FOR ann IN %s
			FOR v IN 1..1 OUTBOUND ann GRAPH '%s'
		        FOR cv IN %s
					FILTER ann._key == '%s'
					FILTER v.graph_id == cv._id
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
	annDelQ = `
		FOR ann IN %s
			FOR v IN 1..1 OUTBOUND ann GRAPH '%s'
				FOR cv IN %s
					FILTER ann.entry_id == '%s'
					FILTER ann.rank == %d
					FILTER ann.is_obsolete == false
					FILTER v.label == '%s'
					FILTER v.graph_id == cv._id
					FILTER cv.metadata.namespace == '%s'
					UPDATE ann WITH { is_obsolete: true } IN %s
	`
)

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
		FOR ann IN %s
			FOR cvt IN 1..1 OUTBOUND ann GRAPH '%s'
				FOR cv IN %s
					FILTER ann.is_obsolete == false
					FILTER cvt.graph_id == cv._id
					SORT ann.created_at DESC
					LIMIT %d
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
	annGetGroupByEntryQ = `
		LET searchedAnnoKeys = (
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
						RETURN MERGE(
							ann, 
							{ ontology: cv.metadata.namespace, tag: v.label }
						)
		)
		LET annoids = (
			FOR akey IN searchedAnnoKeys
				FOR grp IN %s
					FOR ann IN %s
						FILTER akey ANY IN grp.group
						FOR k IN grp.group
							FILTER k == ann._key
							RETURN ann._id
		)
		FOR id IN annoids
			FOR ann IN %s
				FOR v IN 1..1 OUTBOUND id GRAPH '%s'
					FOR cv IN %s
						FILTER ann._id == id
						FILTER v.graph_id == cv._id
						RETURN MERGE(
							ann,
							{ ontology: cv.metadata.namespace, tag: v.label, cvtid: v._id}
						)
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
	annListWithCursorQ = `
		FOR ann IN %s
			FOR cvt IN 1..1 OUTBOUND ann GRAPH '%s'
				FOR cv IN %s
					FILTER ann.is_obsolete == false
					FILTER cvt.graph_id == cv._id
					FILTER ann.created_at <= DATE_ISO8601(%d)
					SORT ann.created_at DESC
					LIMIT %d
					RETURN MERGE(
						ann,
						{ tag: cvt.label, ontology:cv.metadata.namespace }
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
					created_at: DATE_ISO8601(DATE_NOW())
				   } IN @@anno_collection RETURN NEW
		)
		INSERT { _from: n[0]._id, _to: @to } IN @@anno_cv_collection
		INSERT { _from: @prev, _to: n[0]._id } IN @@anno_ver_collection
		RETURN n[0]
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

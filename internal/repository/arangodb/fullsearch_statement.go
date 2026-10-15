package arangodb

const (
	// fullSearchTargetsQ resolves the 6 value-tag cvterm._id values and the
	// 4 ontology cv._id values in one round trip. Missing ontologies or tags
	// are simply absent from the returned maps and default to "" in Go.
	fullSearchTargetsQ = `LET tag_rows = (
  FOR cv IN @@cv_collection
    FILTER cv.metadata.namespace IN @tag_ontologies
    FOR cvt IN @@cvterm_collection
      FILTER cvt.graph_id == cv._id
        AND cvt.deprecated == false
        AND cvt.label IN @tag_labels
      RETURN {
        k: CONCAT(cv.metadata.namespace, ":", cvt.label),
        v: cvt._id
      }
)
LET graph_rows = (
  FOR cv IN @@cv_collection
    FILTER cv.metadata.namespace IN @term_ontologies
    RETURN {
      k: cv.metadata.namespace,
      v: cv._id
    }
)
RETURN {
  tags: ZIP(tag_rows[*].k, tag_rows[*].v),
  graphs: ZIP(graph_rows[*].k, graph_rows[*].v)
}`

	// FullSearchAnnoBranchQ is one branch of the full search statement over
	// the annotation collection. The builder fills it with fmt.Sprintf in
	// this 5-verb order:
	//
	//  1. %s — branch variable (p0..p4, n0..n4, t0, h0)
	//  2. %s — SEARCH value expression on d.value
	//  3. %s — bind parameter name of the required cvterm _id
	//  4. %q — canonical field label reported in each row
	//  5. %s — score expression
	FullSearchAnnoBranchQ = `LET %s = (
  FOR d IN annotation_full_search
    SEARCH d.is_obsolete == false
      AND ANALYZER(STARTS_WITH(d.entry_id, "dbs"), "annotation_search_norm")
      AND %s
    LET has_tag = FIRST(
      FOR cvt, e IN 1..1 OUTBOUND d GRAPH @anno_cvterm_graph
        FILTER e._to == @%s AND cvt.deprecated == false
        RETURN true
    )
    FILTER has_tag == true
    SORT BM25(d) DESC, d.entry_id ASC, d._key ASC
    LIMIT @limit
    RETURN {
      id: d.entry_id,
      f: %q,
      v: NOT_NULL(d.value, ""),
      s: %s
    }
)`

	// FullSearchTermBranchQ is one branch of the full search statement over
	// the cvterm collection. The builder fills it with fmt.Sprintf in the
	// exact same 5-verb order as FullSearchAnnoBranchQ:
	//
	//  1. %s — branch variable (p5..p8, n5..n8, t1..t4, h1..h4)
	//  2. %s — SEARCH label expression on d.label
	//  3. %s — bind parameter name of the required ontology graph_id (cv._id)
	//  4. %q — canonical field label reported in each row
	//  5. %s — score expression
	FullSearchTermBranchQ = `LET %s = (
  FOR d IN annotation_full_search
    SEARCH d.deprecated == false
      AND %s
      AND d.graph_id == @%s
    FOR ann IN 1..1 INBOUND d GRAPH @anno_cvterm_graph
      FILTER ann.is_obsolete == false
        AND STARTS_WITH(LOWER(ann.entry_id), "dbs")
      SORT BM25(d) DESC, ann.entry_id ASC, ann._key ASC
      LIMIT @limit
      RETURN {
        id: ann.entry_id,
        f: %q,
        v: NOT_NULL(d.label, ""),
        s: %s
      }
)`

	// FullSearchMergeQ merges the 28 branches of the full search statement,
	// keeps one best row per entry_id, and returns at most @limit rows.
	FullSearchMergeQ = `LET hits = FLATTEN([%s])
LET best = (
  FOR x IN hits
    COLLECT eid = x.id INTO grp = x
    LET top = FIRST(
      FOR m IN grp
        SORT m.s DESC, m.f ASC, m.v ASC
        RETURN m
    )
    RETURN top
)
FOR x IN best
  SORT x.s DESC, x.id ASC
  LIMIT @limit
  RETURN {
    id: x.id,
    f: x.f,
    v: x.v,
    s: x.s
  }`
)

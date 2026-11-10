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
	// this 6-verb order:
	//
	//  1. %s — branch variable (p0, p1, n0, t0, h0)
	//  2. %s — SEARCH value expression on d.value
	//  3. %s — post-SEARCH value filter expression
	//  4. %s — target cvterm _id list in e._to IN [...]
	//  5. %s — canonical field label expression reported in each row
	//  6. %s — score expression
	FullSearchAnnoBranchQ = `LET %s = (
  FOR d IN annotation_full_search
    SEARCH d.is_obsolete == false
      AND ANALYZER(STARTS_WITH(d.entry_id, "dbs"), "annotation_search_norm")
      AND %s
    FILTER %s
    LET f = FIRST(
      FOR cvt, e IN 1..1 OUTBOUND d GRAPH @anno_cvterm_graph
        FILTER e._to IN [%s] AND cvt.deprecated == false
        RETURN %s
    )
    FILTER f != null
    SORT BM25(d) DESC, d.entry_id ASC, d._key ASC
    LIMIT @limit
    RETURN {
      id: d.entry_id,
      f: f,
      v: NOT_NULL(d.value, ""),
      s: %s
    }
)`

	// FullSearchTermBranchQ is one branch of the full search statement over
	// the cvterm collection. The builder fills it with fmt.Sprintf in this
	// 5-verb order:
	//
	//  1. %s — branch variable (p2..p5, n1..n4, t1..t4, h1..h4)
	//  2. %s — SEARCH label expression on d.label
	//  3. %s — bind parameter name of the required ontology graph_id (cv._id)
	//  4. %s — quoted canonical field label reported in each row
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
        f: %s,
        v: NOT_NULL(d.label, ""),
        s: %s
      }
)`

	// FullSearchMergeQ merges the 21 branches of the full search statement,
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

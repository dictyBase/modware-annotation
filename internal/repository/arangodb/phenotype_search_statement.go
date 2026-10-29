package arangodb

const (
	// phenotypeSearchTargetsQ resolves the 3 phenotype ontology cv._id values
	// in one round trip. Missing ontologies are absent from the returned map
	// and default to "" in Go.
	phenotypeSearchTargetsQ = `LET graph_rows = (
  FOR cv IN @@cv_collection
    FILTER cv.metadata.namespace IN @term_ontologies
    RETURN {
      k: cv.metadata.namespace,
      v: cv._id
    }
)
RETURN {
  graphs: ZIP(graph_rows[*].k, graph_rows[*].v)
}`

	// PhenotypeSearchBranchQ is one branch of the phenotype search statement
	// over the cvterm collection with an inbound traversal to active strain
	// annotations. The builder fills it with fmt.Sprintf in this 4-verb order:
	//
	//  1. %s — branch variable (p0..p1, n0..n1, t0..t3, h0..h3)
	//  2. %s — SEARCH expression on d.label, d.metadata.synonyms.value,
	//          d.metadata.definition.value, or d.metadata.comments
	//  3. %q — canonical field label reported in each row ("label",
	//          "synonym", "definition", or "comment")
	//  4. %s — score band expression
	PhenotypeSearchBranchQ = `LET %s = (
  FOR d IN annotation_full_search
    SEARCH d.deprecated == false
      AND %s
      AND d.graph_id IN [@phenotype_graph_id, @env_graph_id, @assay_graph_id]
    FOR ann IN 1..1 INBOUND d GRAPH @anno_cvterm_graph
      FILTER ann.is_obsolete == false
        AND STARTS_WITH(LOWER(ann.entry_id), "dbs")
      SORT BM25(d) DESC, ann.entry_id ASC, ann._key ASC
      LIMIT @limit
      RETURN {
        id: ann.entry_id,
        f: %q,
        name: NOT_NULL(d.label, ""),
        def: NOT_NULL(d.metadata.definition.value, ""),
        s: %s
      }
)`

	// PhenotypeSearchMergeQ merges the 12 branches of the phenotype search
	// statement, keeps one best row per strain entry_id, and returns at most
	// @limit rows.
	PhenotypeSearchMergeQ = `LET hits = FLATTEN([%s])
LET best = (
  FOR x IN hits
    COLLECT eid = x.id INTO grp = x
    LET top = FIRST(
      FOR m IN grp
        SORT m.s DESC, m.f ASC, m.name ASC
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
    name: x.name,
    def: x.def,
    s: x.s
  }`
)

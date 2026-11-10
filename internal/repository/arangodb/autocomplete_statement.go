package arangodb

const (
	// AutocompleteBranchQ is one branch of the annotation autocomplete
	// statement over the annotation collection. The builder fills it with
	// fmt.Sprintf in this verb order:
	//
	//  1. %s — the branch variable (p_synonym, p_genotype, or n_value)
	//  2. %s — the SEARCH value expression
	//  3. %s — the post-SEARCH value filter expression
	//  4. %s — the target cvterm _id list in e._to IN [...]
	//  5. %s — the field label expression reported in each row
	//  6. %s — the score expression
	//
	// It requires the bind parameters q, th, anno_cvterm_graph,
	// synonym_term_id, genotype_term_id, and limit. Because the cvterm _id
	// values are resolved ahead of the query, each branch checks
	// e._to IN [...] on the 1-hop OUTBOUND traversal without scanning
	// @@cv_collection, and the fuzzy stage scans d.value once for both
	// synonym and genotype.
	AutocompleteBranchQ = `LET %s = (
  FOR d IN annotation_autocomplete
    SEARCH d.is_obsolete == false
      AND ANALYZER(STARTS_WITH(d.entry_id, "dbs"), "annotation_autocomplete_norm")
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

	// AutocompleteMergeQ merges the 3 branches of the autocomplete
	// statement, keeps one best row per entry_id, and returns the top rows.
	// The builder fills it with one verb:
	//
	//  1. %s — the comma-separated list of the 3 branch variables
	//
	// It requires the bind parameter limit.
	AutocompleteMergeQ = `LET hits = FLATTEN([%s])
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

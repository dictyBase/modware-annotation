package arangodb

const (
	// AutocompleteBranchQ is one branch of the annotation autocomplete
	// statement over the annotation collection. The builder fills it with
	// fmt.Sprintf in this verb order:
	//
	//  1. %s — the branch variable (p_<tag> for prefix, n_<tag> for fuzzy)
	//  2. %s — the SEARCH value expression
	//  3. %s — the bind parameter name of the required cvterm _id
	//          (synonym_term_id or genotype_term_id)
	//  4. %q — the field label that the branch reports ("synonym" or "genotype")
	//  5. %s — the score expression
	//
	// It requires the bind parameters q, th, anno_cvterm_graph,
	// synonym_term_id, genotype_term_id, and limit. Because the cvterm _id
	// is resolved ahead of the query, the branch checks e._to == @<tag>_term_id
	// on the 1-hop OUTBOUND traversal without scanning @@cv_collection.
	AutocompleteBranchQ = `LET %s = (
  FOR d IN annotation_autocomplete
    SEARCH d.is_obsolete == false
      AND ANALYZER(STARTS_WITH(d.entry_id, "dbs"), "annotation_autocomplete_norm")
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

	// AutocompleteMergeQ merges the 4 branches of the autocomplete
	// statement, keeps one best row per entry_id, and returns the top rows.
	// The builder fills it with one verb:
	//
	//  1. %s — the comma-separated list of the 4 branch variables
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

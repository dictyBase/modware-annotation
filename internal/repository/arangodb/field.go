package arangodb

// FilterMap provides mapping of filter attributes to database fields.
func FilterMap() map[string]string {
	return map[string]string{
		"entry_id":   "ann.entry_id",
		"value":      annValueField,
		"created_by": "ann.created_by",
		"version":    "ann.version",
		"rank":       "ann.rank",
		"tag":        cvtLabelField,
		"ontology":   "cv.metadata.namespace",
	}
}

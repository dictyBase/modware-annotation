package arangodb

// FilterMap maps filter attributes to database fields
var FilterMap = map[string]string{
	"entry_id":   "ann.entry_id",
	"value":      "ann.value",
	"created_by": "ann.created_by",
	"version":    "ann.version",
	"tag":        "cvt.label",
	"ontology":   "cv.metadata.namespace",
}

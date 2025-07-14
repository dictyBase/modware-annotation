This file is a merged representation of a subset of the codebase, containing files not matching ignore patterns, combined into a single document by Repomix.
The content has been processed where comments have been removed, content has been compressed (code blocks are separated by ⋮---- delimiter).

# File Summary

## Purpose
This file contains a packed representation of a subset of the repository's contents that is considered the most important context.
It is designed to be easily consumable by AI systems for analysis, code review,
or other automated processes.

## File Format
The content is organized as follows:
1. This summary section
2. Repository information
3. Directory structure
4. Repository files (if enabled)
5. Multiple file entries, each consisting of:
  a. A header with the file path (## File: path/to/file)
  b. The full contents of the file in a code block

## Usage Guidelines
- This file should be treated as read-only. Any changes should be made to the
  original repository files, not this packed version.
- When processing this file, use the file path to distinguish
  between different files in the repository.
- Be aware that this file may contain sensitive information. Handle it with
  the same level of security as you would the original repository.

## Notes
- Some files may have been excluded based on .gitignore rules and Repomix's configuration
- Binary files are not included in this packed representation. Please refer to the Repository Structure section for a complete list of file paths, including binary files
- Files matching these patterns are excluded: **/**organism**
- Files matching patterns in .gitignore are excluded
- Files matching default ignore patterns are excluded
- Code comments have been removed from supported file types
- Content has been compressed - code blocks are separated by ⋮---- delimiter
- Files are sorted by Git change count (files with more changes are at the bottom)

# Directory Structure
```
feature_annotation.proto
```

# Files

## File: feature_annotation.proto
```protobuf
syntax = "proto3";

package dictybase.feature_annotation;

import "buf/validate/validate.proto";
import "google/protobuf/empty.proto";
import "google/protobuf/timestamp.proto";

option cc_enable_arenas = true;
option go_package = "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation;feature_annotation";
option java_multiple_files = true;
option java_outer_classname = "FeatureAnnotationProto";
option java_package = "org.dictybase.feature";
option objc_class_prefix = "DICTYAPI";

// The feature annotation service specification
service FeatureAnnotationService {
  // Create a feature annotation
  rpc CreateFeatureAnnotation(NewFeatureAnnotation) returns (FeatureAnnotation) {}
  // Retrieves the specified feature annotation
  rpc GetFeatureAnnotation(FeatureAnnotationId) returns (FeatureAnnotation) {}
  // Retrieves the specified feature annotation by its name
  rpc GetFeatureAnnotationByName(FeatureName) returns (FeatureAnnotation) {}
  // Update an existing feature annotation. Any included tags will replace all
  // existing tags (identical behavior to SetTags)
  rpc UpdateFeatureAnnotation(FeatureAnnotationUpdate) returns (FeatureAnnotation) {}
  // Delete an existing feature annotation
  rpc DeleteFeatureAnnotation(DeleteFeatureAnnotationRequest) returns (google.protobuf.Empty) {}
  // Add tag to an existing feature annotation
  rpc AddTag(AddTagRequest) returns (FeatureAnnotation) {}
  // Add multiple tags to an existing feature annotation
  rpc AddTags(AddTagsRequest) returns (FeatureAnnotation) {}
  // Replace all tags for a feature annotation (idempotent full-update)
  rpc SetTags(SetTagsRequest) returns (FeatureAnnotation) {}
  // Update an existing tag in a feature annotation
  rpc UpdateTag(UpdateTagRequest) returns (FeatureAnnotation) {
    option deprecated = true;
  }
  // Remove a tag from a feature annotation
  rpc RemoveTag(RemoveTagRequest) returns (FeatureAnnotation) {
    option deprecated = true;
  }
  // Remove a tag from a feature annotation
  rpc RemoveTags(RemoveTagsRequest) returns (FeatureAnnotation) {}
  // Retrieves a list of feature annotations by PubMed ID
  rpc ListFeatureAnnotationsByPubmedId(PubmedId) returns (FeatureAnnotationCollection) {}
  // Retrieves a list of feature annotations by DOI (Digital Object Identifier)
  rpc ListFeatureAnnotationsByDOI(DOI) returns (FeatureAnnotationCollection) {}
}

// FeatureAnnotation represents a complete feature annotation record with all
// its metadata.
message FeatureAnnotation {
  string type = 1;
  // unique identifier for the feature annotation
  string id = 2;
  FeatureAnnotationAttributes attributes = 3;
  // email id of the user who created the content
  string created_by = 4 [(buf.validate.field).cel = {
    id: "valid_email"
    message: "email must be a valid email"
    expression: "this.isEmail()"
  }];
  // email id of the user who updated the content
  string updated_by = 5 [
    (buf.validate.field).ignore_empty = true,
    (buf.validate.field).cel = {
      id: "valid_email"
      message: "email must be a valid email"
      expression: "this.isEmail()"
    }
  ];
  // Timestamp for creation and update
  google.protobuf.Timestamp created_at = 6;
  google.protobuf.Timestamp updated_at = 7;
  // Toggle the obsolete status
  bool is_obsolete = 8;
  int64 version = 9 [deprecated = true];
}

// FeatureAnnotationAttributes defines the core properties and metadata of a
// feature annotation.
message FeatureAnnotationAttributes {
  // Short human readable textual name
  string name = 1 [(buf.validate.field).required = true];
  // Alternate list of names
  repeated string synonyms = 2;
  // List of publications(doi identifiers)
  repeated string publications = 3;
  // List of pubmed id
  repeated string pubmed = 4;
  repeated Dbxref dbxrefs = 5 [deprecated = true];
  // Cross references to other databases
  repeated DbLink dblinks = 6;
  // Bucket of key value pair data
  repeated TagProperty properties = 7;
}

// FeatureAnnotationUpdate contains the fields needed to update an existing
// feature annotation.
message FeatureAnnotationUpdate {
  string type = 1;
  string id = 2 [(buf.validate.field).required = true];
  FeatureAnnotationAttributes attributes = 3;
  // email id of the user who updated the content
  string updated_by = 4 [(buf.validate.field).cel = {
    id: "valid_email"
    message: "email must be a valid email"
    expression: "this.isEmail()"
  }];
  // Toggle the obsolete status
  bool is_obsolete = 5;
}

// NewFeatureAnnotation contains all the information needed to create a new
// feature annotation.
message NewFeatureAnnotation {
  string type = 1;
  FeatureAnnotationAttributes attributes = 2 [(buf.validate.field).required = true];
  // email id of the user who created the content
  string created_by = 3 [(buf.validate.field).cel = {
    id: "valid_email"
    message: "email must be a valid email"
    expression: "this.isEmail()"
  }];
  // Timestamp for creation and update
  google.protobuf.Timestamp created_at = 4;
  google.protobuf.Timestamp updated_at = 5;
  // Toggle the obsolete status
  bool is_obsolete = 6;
  int64 version = 7 [deprecated = true];
  // unique identifier for the feature annotation
  string id = 8 [(buf.validate.field).required = true];
  string updated_by = 9 [
    (buf.validate.field).cel = {
      id: "valid_email"
      message: "email must be a valid email"
      expression: "this.isEmail()"
    },
    (buf.validate.field).ignore = IGNORE_IF_UNPOPULATED
  ];
}

// FeatureAnnotationId is used to uniquely identify a feature annotation.
message FeatureAnnotationId {
  // unique identifier for the feature annotation
  string id = 1 [(buf.validate.field).required = true];
}

// FeatureName is used to specify a feature name for retrieval.
message FeatureName {
  // Feature name to search for
  string name = 1 [(buf.validate.field).required = true];
}

// DeleteFeatureAnnotationRequest specifies a feature annotation to delete and
// how to delete it.
message DeleteFeatureAnnotationRequest {
  // unique identifier for the feature annotation
  string id = 1 [(buf.validate.field).required = true];
  // flag to indicate whether the entry will be wiped or turned obsolete(soft delete)
  bool purge = 2;
}

// DbLink represents a reference to an external bioinformatics database entry.
message DbLink {
  // Identifier of the linked database
  string primary_id = 1 [(buf.validate.field).required = true];
  // a number which differentiates between versions of
  // the same object. Higher numbers are considered to be
  // later and more relevant
  int64 version = 2;
  // Source database
  string database = 3 [(buf.validate.field).required = true];
  // Type of this dblink, for example 'protein'
  string linktype = 4;
  // URL that is associated with this link
  string url = 5 [(buf.validate.field).string.uri = true];
  // A short name that is a possible alternative to the database name which
  // could be used for display.
  string label = 6;
}

// TagProperty represents a key-value pair structure with metadata for storing
// custom attributes.
message TagProperty {
  string tag = 1 [(buf.validate.field).required = true];
  string value = 2 [(buf.validate.field).required = true];
  // email id of the user who created the tag
  string created_by = 3 [(buf.validate.field).cel = {
    id: "valid_email"
    message: "email must be a valid email"
    expression: "this.isEmail()"
  }];
  // email id of the user who updated the tag
  string updated_by = 4 [
    (buf.validate.field).cel = {
      id: "valid_email"
      message: "email must be a valid email"
      expression: "this.isEmail()"
    },
    (buf.validate.field).ignore = IGNORE_IF_UNPOPULATED
  ];
  // Timestamp for creation and update
  google.protobuf.Timestamp created_at = 5;
  google.protobuf.Timestamp updated_at = 6;
}

// TagPropertyCreate contains the minimal information needed to create a new tag
// property.
message TagPropertyCreate {
  string tag = 1 [(buf.validate.field).required = true];
  string value = 2 [(buf.validate.field).required = true];
  // email id of the user who created the tag
  string created_by = 3 [(buf.validate.field).cel = {
    id: "valid_email"
    message: "email must be a valid email"
    expression: "this.isEmail()"
  }];
  // Timestamp for creation
  google.protobuf.Timestamp created_at = 4;
}

// TagPropertyUpdate contains the information needed to update an existing tag
// property.
message TagPropertyUpdate {
  option deprecated = true;
  string tag = 1 [
    deprecated = true,
    (buf.validate.field).required = true
  ];
  string value = 2 [
    deprecated = true,
    (buf.validate.field).required = true
  ];
  // email id of the user who created the tag
  string updated_by = 3 [
    deprecated = true,
    (buf.validate.field).cel = {
      id: "valid_email"
      message: "email must be a valid email"
      expression: "this.isEmail()"
    }
  ];
  google.protobuf.Timestamp updated_at = 4 [deprecated = true];
}

// Dbxref represents a deprecated cross-reference to an external database.
message Dbxref {
  // Identifier
  string dbxref_id = 1 [(buf.validate.field).required = true];
  int64 version = 2 [(buf.validate.field).ignore = IGNORE_IF_UNPOPULATED];
  // Source database
  string database = 3 [(buf.validate.field).required = true];
}

// AddTagRequest specifies a feature annotation and a tag to add to it.
message AddTagRequest {
  // ID of feature annotation to modify
  string id = 1 [(buf.validate.field).required = true];
  // Tag to be added
  TagPropertyCreate tag = 2 [(buf.validate.field).required = true];
}

// AddTagsRequest specifies a feature annotation and multiple tags to add to it.
message AddTagsRequest {
  // ID of feature annotation to modify
  string id = 1 [(buf.validate.field).required = true];
  // Tags to be added
  repeated TagPropertyCreate tags = 2 [(buf.validate.field).required = true];
}

// SetTagsRequest specifies a feature annotation and the complete set of tags to
// replace all existing tags.
message SetTagsRequest {
  // ID of feature annotation to modify
  string id = 1 [(buf.validate.field).required = true];
  // Complete set of tags to replace all existing tags
  repeated TagPropertyCreate tags = 2 [(buf.validate.field).required = true];
}

// UpdateTagRequest specifies a feature annotation and updated tag values.
message UpdateTagRequest {
  option deprecated = true;
  // ID of feature annotation to modify
  string id = 1 [
    deprecated = true,
    (buf.validate.field).required = true
  ];
  // Updated tag values
  TagPropertyUpdate tag = 2 [
    deprecated = true,
    (buf.validate.field).required = true
  ];
}

// RemoveTagRequest specifies a feature annotation and a tag to remove from it.
message RemoveTagRequest {
  option deprecated = true;
  // ID of feature annotation to modify
  string id = 1 [deprecated = true];
  // Tag to remove
  string tag = 2 [deprecated = true];
}

// RemoveTagsRequest specifies a feature annotation and a tag to remove from it.
message RemoveTagsRequest {
  // ID of feature annotation to modify
  string id = 1 [(buf.validate.field).required = true];
  // Tag name to remove
  string tag = 2 [(buf.validate.field).required = true];
  // Tag value to remove
  string value = 3 [(buf.validate.field).required = true];
}

// PubmedId is used to specify a PubMed ID for retrieving feature annotations.
message PubmedId {
  // PubMed ID to search for
  string id = 1 [(buf.validate.field).required = true];
}

// FeatureAnnotationCollection contains multiple feature annotations.
message FeatureAnnotationCollection {
  // Collection of feature annotations
  repeated FeatureAnnotation data = 1;
}

// DOI is used to specify a Digital Object Identifier for retrieving feature annotations.
message DOI {
  // DOI to search for (e.g. "10.1234/abcd.1234")
  string id = 1 [
    (buf.validate.field).required = true,
    (buf.validate.field).string.pattern = "^10\\.[0-9]{4,}(\\.\\w+)*\\/[-._;()/:a-zA-Z0-9]+$"
  ];
}
```

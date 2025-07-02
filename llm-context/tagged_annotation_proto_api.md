This file is a merged representation of the entire codebase, combined into a single document by Repomix.
The content has been processed where comments have been removed, content has been compressed (code blocks are separated by ⋮---- delimiter).

# File Summary

## Purpose
This file contains a packed representation of the entire repository's contents.
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
- Files matching patterns in .gitignore are excluded
- Files matching default ignore patterns are excluded
- Code comments have been removed from supported file types
- Content has been compressed - code blocks are separated by ⋮---- delimiter
- Files are sorted by Git change count (files with more changes are at the bottom)

# Directory Structure
```
annotation.proto
```

# Files

## File: annotation.proto
```protobuf
syntax = "proto3";

package dictybase.annotation;

import "buf/validate/validate.proto";
import "dictybase/api/upload/file.proto";
import "google/protobuf/empty.proto";
import "google/protobuf/timestamp.proto";

option cc_enable_arenas = true;
option go_package = "github.com/dictyBase/go-genproto/dictybaseapis/annotation;annotation";
option java_multiple_files = true;
option java_outer_classname = "AnnotationProto";
option java_package = "org.dictybase.annotation";
option objc_class_prefix = "DICTYAPI";

// The tagged annotation service specification
service TaggedAnnotationService {
  // Retrieves the specified tagged annotation
  rpc GetAnnotation(AnnotationId) returns (TaggedAnnotation) {}
  // Retrieves a single tagged annotation associated with a specific entry
  rpc GetEntryAnnotation(EntryAnnotationRequest) returns (TaggedAnnotation) {}
  // List tagged annotations using pagination, ten entries are retrieved by default
  rpc ListAnnotations(ListParameters) returns (TaggedAnnotationCollection) {}
  // Create a tagged annotation
  rpc CreateAnnotation(NewTaggedAnnotation) returns (TaggedAnnotation) {}
  // Update an existing annotation, in this case a new annotation entry is
  // created with a link to the previous annotation(copy on write).
  rpc UpdateAnnotation(TaggedAnnotationUpdate) returns (TaggedAnnotation) {}
  // Delete an existing annotation
  rpc DeleteAnnotation(DeleteAnnotationRequest) returns (google.protobuf.Empty) {}
  // Creates an annotation group from bunch of existing tagged annotations.
  rpc CreateAnnotationGroup(AnnotationIdList) returns (TaggedAnnotationGroup) {}
  // Retrieves an annotation group
  rpc GetAnnotationGroup(GroupEntryId) returns (TaggedAnnotationGroup) {}
  // Adds an existing annotation into an existing annotation group
  rpc AddToAnnotationGroup(AnnotationGroupId) returns (TaggedAnnotationGroup) {}
  // Remove an annotation group
  rpc DeleteAnnotationGroup(GroupEntryId) returns (google.protobuf.Empty) {}
  // List tagged annotation groups using pagination, ten entries are retrieved by default
  rpc ListAnnotationGroups(ListGroupParameters) returns (TaggedAnnotationGroupCollection) {}
  // Retrieves tag information
  rpc GetAnnotationTag(TagRequest) returns (AnnotationTag) {}
  // Upload obojson formatted file through client side streaming
  rpc OboJSONFileUpload(stream dictybase.api.upload.FileUploadRequest) returns (dictybase.api.upload.FileUploadResponse) {}
}

message AnnotationGroupId {
  // unique identifier for the annotation
  string id = 1 [(buf.validate.field).required = true];
  // unique identifier of a group
  string group_id = 2 [(buf.validate.field).required = true];
}

message AnnotationId {
  // unique identifier for the annotation
  string id = 1 [(buf.validate.field).required = true];
}

// Identifiers for grouping list of tagged annotations
message AnnotationIdList {
  // list of unique identifiers
  repeated string ids = 1 [(buf.validate.field).repeated = {min_items: 1}];
}

message GroupEntryId {
  // unique identifier of a group
  string group_id = 1 [(buf.validate.field).required = true];
}

// Group of tagged annotations
message TaggedAnnotationGroup {
  message Data {
    // resource name, by default should be annotation
    string type = 1;
    // unique identifier for the annotation
    string id = 2;
    TaggedAnnotationAttributes attributes = 3;
  }
  repeated Data data = 1;
  // unique identifier for the annotation group
  string group_id = 2;
  // Timestamp for creation an update
  google.protobuf.Timestamp created_at = 3 [(buf.validate.field).required = true];
  google.protobuf.Timestamp updated_at = 4 [(buf.validate.field).required = true];
}

// List of tagged annotation groups
message TaggedAnnotationGroupCollection {
  message Data {
    // resource name, by default it should be annotation group
    string type = 1;
    TaggedAnnotationGroup group = 2;
  }
  repeated Data data = 1;
  Meta meta = 2 [(buf.validate.field).required = true];
}

// Definition for various fields that are needed for fetching annotation for an
// entry. The tag, ontology and entry_id must be provided, version and rank are
// optional and their default values are used.
message EntryAnnotationRequest {
  // An identifiable tagname for the annotation, primarily
  // a structured tag, generally an ontology term.
  string tag = 1 [(buf.validate.field).required = true];
  // unique identifier of a biological entity that is being annotated
  string entry_id = 2 [(buf.validate.field).required = true];
  // Name of ontology in which the tag name is taken
  string ontology = 3 [(buf.validate.field).required = true];
  // Ordering of annotation when an entry has multiple annotations with
  // identical tag from the same ontology. By default, rank 0 is assumed.
  int64 rank = 4;
  // Status for active or retired annotation. Active annotation is chosen by default.
  bool is_obsolete = 5;
}

message DeleteAnnotationRequest {
  // unique identifier for the annotation
  string id = 1 [(buf.validate.field).required = true];
  // flag to indicate whether the entry will be wiped or turned obsolete(soft delete)
  bool purge = 2;
}

// Definition of various fields needed to fetch a tag information
message TagRequest {
  // human readable name
  string name = 1 [(buf.validate.field).required = true];
  // ontology to which this tag belong
  string ontology = 2 [(buf.validate.field).required = true];
  // status for active or retired tag
  bool is_obsolete = 3;
}

// List of paginated tagged annotations
message TaggedAnnotationCollection {
  message Data {
    // resource name, by default should be annotation
    string type = 1;
    // unique identifier for the annotation
    string id = 2;
    TaggedAnnotationAttributes attributes = 3;
  }
  repeated Data data = 1;
  Meta meta = 3 [(buf.validate.field).required = true];
}

// Definition of an tag value based biological annotation where the tag always
// represents a term from ontology.
message TaggedAnnotation {
  message Data {
    // resource name, by default should be annotation
    string type = 1;
    // unique identifier for the annotation
    string id = 2;
    TaggedAnnotationAttributes attributes = 3;
  }
  Data data = 1 [(buf.validate.field).required = true];
}

// Definition of annotation tag
message AnnotationTag {
  // tag identifier
  string id = 1;
  // human readable name
  string name = 2;
  // ontology to which this tag belong
  string ontology = 3;
  // status for active or retired tag
  bool is_obsolete = 4;
}

// Definition of various tagged annotation attributes
message TaggedAnnotationAttributes {
  // annotation in plain text format
  string value = 1 [(buf.validate.field).required = true];
  // serialized text content in a format recognized by frontend rich text
  // editor
  string editable_value = 2;
  // Unique identifier(generally email) of the user who created the annotation
  // Timestamp for creation
  string created_by = 3 [(buf.validate.field).cel = {
    id: "valid_email"
    message: "email must be a valid email"
    expression: "this.isEmail()"
  }];
  google.protobuf.Timestamp created_at = 4 [(buf.validate.field).required = true];
  // An identifiable tagname for the annotation, primarily
  // a structured tag, generally an ontology term.
  string tag = 5 [(buf.validate.field).required = true];
  // version refers to the current version no
  int64 version = 6 [(buf.validate.field).int64 = {gt: 0}];
  // unique identifier of a biological entity that is being annotated
  string entry_id = 7 [(buf.validate.field).required = true];
  // Name of ontology in which the tag name is taken
  string ontology = 8 [(buf.validate.field).required = true];
  // Ordering of annotation when an entry has multiple annotations with
  // identical tag from the same ontology.
  int64 rank = 9;
  // Status for active or retired annotation
  bool is_obsolete = 10;
}

// Definition for creating a new tagged annotation
message NewTaggedAnnotation {
  message Data {
    // resource name, by default should be annotation
    string type = 1;
    NewTaggedAnnotationAttributes attributes = 2;
  }
  Data data = 1 [(buf.validate.field).required = true];
}

// NewTaggedAnnotation defines attributes for creating a new annotation
message NewTaggedAnnotationAttributes {
  // annotation in plain text format
  string value = 1 [(buf.validate.field).required = true];
  // serialized text content in a format recognized by frontend rich text
  // editor
  string editable_value = 2;
  // Unique identifier(generally email) of the user who created the annotation
  string created_by = 3 [(buf.validate.field).cel = {
    id: "valid_email"
    message: "email must be a valid email"
    expression: "this.isEmail()"
  }];
  // An identifiable tagname for the annotation, primarily
  // a structured tag, generally an ontology term.
  string tag = 4 [(buf.validate.field).required = true];
  // unique identifier of a biological entity that is being annotated
  string entry_id = 5 [(buf.validate.field).required = true];
  // Name of ontology from which the tag name is taken
  string ontology = 6 [(buf.validate.field).required = true];
  // Ordering of annotation when an entry has multiple annotations with
  // identical tag from the same ontology. By default, rank 0 is used.
  int64 rank = 7;
}

// Definition for updating an existing annotation
message TaggedAnnotationUpdate {
  message Data {
    // resource name, by default should be annotation
    string type = 1;
    // unique identifier for the annotation
    string id = 2 [(buf.validate.field).required = true];
    TaggedAnnotationUpdateAttributes attributes = 3;
  }
  Data data = 1 [(buf.validate.field).required = true];
}

// TaggedUpdateAnnotation defines attributes for updating an existing annotation
message TaggedAnnotationUpdateAttributes {
  // annotation in plain text format
  string value = 1 [(buf.validate.field).required = true];
  // serialized text content in a format recognized by frontend rich text
  // editor
  string editable_value = 2 [(buf.validate.field).required = true];
  // Unique identifier(generally email) of the user who created the annotation
  string created_by = 3 [(buf.validate.field).cel = {
    id: "valid_email"
    message: "email must be a valid email"
    expression: "this.isEmail()"
  }];
}

// ListParameters defines fields for manipulating output of TaggedAnnotation collection
message ListParameters {
  // A unique pointer to the next set of result in the list
  int64 cursor = 1;
  // Maximum number of records that can be fetch per request
  int64 limit = 2;

  // The `filter` field restricts the data returned by the collection. It
  // accepts one or more filtering conditions using the syntax:
  //     field_name operator expression
  //
  // Available fields for filtering from `AnnotationAttributes`:
  //   * entry_id    - Identifier of the annotated entry (string)
  //   * value       - Annotation text content (string)
  //   * created_by  - Email of the creator (string)
  //   * tag         - Ontology term used as tag (string)
  //   * ontology    - Source ontology name (string)
  //   * version     - Version number (number)
  //   * rank        - Ordering of annotation (number)
  //   * is_obsolete - Status of annotation (boolean)
  //
  // String operators:
  //   * =~    Contains substring
  //   * !~    Does not contain substring
  //   * ===   Equals exactly
  //   * !==   Not equals
  //
  // Numeric operators:
  //   * ==    Equals
  //   * !=    Not equals
  //   * >     Greater than
  //   * <     Less than
  //   * =<    Less than or equal to
  //   * >=    Greater than or equal to
  //
  // Boolean operators:
  //   * ==    Equals
  //   * !=    Not equals
  //
  // Examples:
  //   filter: "created_by===caboose@abc.com"
  //   filter: "entry_id===DDB_G4839483"
  //   filter: "value=~actin"
  //   filter: "tag===GO:0005634"
  //   filter: "ontology===cellular_component"
  //   filter: "version>3"
  //   filter: "rank==0"
  //   filter: "is_obsolete==false"
  //   filter: "value!~pseudogene"
  //   filter: "created_by!==anonymous@dictybase.org"
  //
  // Combine filters with boolean operators:
  //   * OR: represented by comma (,)
  //   * AND: represented by semicolon (;)
  //   * AND takes precedence over OR
  //
  // Example of combined filters:
  //   filter: "value=~cytoskeleton;tag===cell membrane;ontology===cellular"
  //   filter: "entry_id===DDB_G0285418;is_obsolete==false"
  //   filter: "created_by===curator@dictybase.org;version>2;rank==0"
  //   filter: "tag===GO:0005634,tag===GO:0005737" (entries with nuclear OR cytoplasmic localization)
  //   filter: "ontology===molecular_function;value=~transport" (transport-related functions)
  //
  // Note: URL-reserved characters must be properly encoded in HTTP requests.
  //
  string filter = 3 [(buf.validate.field).required = true];
}

// ListGroupParameters defines fields for manipulating output of TaggedAnnotationGroupCollection collection
message ListGroupParameters {
  // A unique pointer to the next set of result in the list
  int64 cursor = 1;
  // Maximum number of records that can be fetch per request
  int64 limit = 2;
  // The `filter` field restricts the data return by the collection. To use
  // it, supply one or multiple allowed fields to filter followed
  // by a filter expression. It uses the following syntax...
  //        field_name operator expression
  //
  // The following fields of `AnnotationAttributes` definition are allowed to
  // be used for filtering
  //   * entry_id    - The entry that is being annotated (string)
  //   * created_by  - Email id of the user (string)
  //   * tag         - Tag name, a term from an ontology (string).
  //   * ontology    - Ontology that provides the tag names (string).
  //   * rank        - Ordering of annotation (number).
  //
  // operator - Defines the type of filter match to use. It could be any of
  // the following four and all of them should be URL-encoded for http request.
  //
  //        Operators for strings
  //              =~    Contains substring
  //              !~   Not contains substring
  //              ===  Equals
  //              !==  Not equals
  //
  //        Operators for numbers
  //              ==  Equals
  //              !=  Not equals
  //              >   Greater than
  //              <   Less than
  //              =<  Less than equal to
  //              >=  Greater than equal to
  //
  // expression - The value that will be included or excluded from the
  // result. URL-reserved characters must be URL-encoded for http request.
  //
  //           filter: "created_by==caboose@abc.com"
  //           filter: "entry_id==DDB_G4839483"
  //           filter: "tag==growth"
  //
  // Filter can be combined using OR or AND boolean logic.
  //   * The OR is represented using a comma(,).
  //   * The AND is represented using a semi-colon(;).
  //   * AND and OR operators can be combined and AND takes precedence over OR.
  //
  //           filter: "tag~cytoskeletion;entry_id==DDB_G4839783;ontology==cellular"
  //           filter: "tag~membrane;entry_id==DDB_G4839783;ontology==cellular;rank=0"
  //
  string filter = 3;
}

message Meta {
  // A unique pointer to the next set of result in the collection. Set the
  // cursor value parameter to the value of next_cursor to retrieve the next
  // set of collection using the same method
  int64 next_cursor = 1;
  // Maximum number of records that can be fetch per request
  int64 limit = 2;
}
```

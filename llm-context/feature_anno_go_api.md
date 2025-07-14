This file is a merged representation of a subset of the codebase, containing specifically included files and files not matching ignore patterns, combined into a single document by Repomix.
The content has been processed where comments have been removed, empty lines have been removed, content has been compressed (code blocks are separated by ⋮---- delimiter).

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
- Only files matching these patterns are included: **/**grpc**
- Files matching these patterns are excluded: **/**organism**
- Files matching patterns in .gitignore are excluded
- Files matching default ignore patterns are excluded
- Code comments have been removed from supported file types
- Empty lines have been removed from all files
- Content has been compressed - code blocks are separated by ⋮---- delimiter
- Files are sorted by Git change count (files with more changes are at the bottom)

# Directory Structure
```
feature_annotation_grpc.pb.go
```

# Files

## File: feature_annotation_grpc.pb.go
```go
package feature_annotation
import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)
⋮----
context "context"
grpc "google.golang.org/grpc"
codes "google.golang.org/grpc/codes"
status "google.golang.org/grpc/status"
emptypb "google.golang.org/protobuf/types/known/emptypb"
⋮----
const _ = grpc.SupportPackageIsVersion7
const (
	FeatureAnnotationService_CreateFeatureAnnotation_FullMethodName          = "/dictybase.feature_annotation.FeatureAnnotationService/CreateFeatureAnnotation"
	FeatureAnnotationService_GetFeatureAnnotation_FullMethodName             = "/dictybase.feature_annotation.FeatureAnnotationService/GetFeatureAnnotation"
	FeatureAnnotationService_GetFeatureAnnotationByName_FullMethodName       = "/dictybase.feature_annotation.FeatureAnnotationService/GetFeatureAnnotationByName"
	FeatureAnnotationService_UpdateFeatureAnnotation_FullMethodName          = "/dictybase.feature_annotation.FeatureAnnotationService/UpdateFeatureAnnotation"
	FeatureAnnotationService_DeleteFeatureAnnotation_FullMethodName          = "/dictybase.feature_annotation.FeatureAnnotationService/DeleteFeatureAnnotation"
	FeatureAnnotationService_AddTag_FullMethodName                           = "/dictybase.feature_annotation.FeatureAnnotationService/AddTag"
	FeatureAnnotationService_AddTags_FullMethodName                          = "/dictybase.feature_annotation.FeatureAnnotationService/AddTags"
	FeatureAnnotationService_SetTags_FullMethodName                          = "/dictybase.feature_annotation.FeatureAnnotationService/SetTags"
	FeatureAnnotationService_UpdateTag_FullMethodName                        = "/dictybase.feature_annotation.FeatureAnnotationService/UpdateTag"
	FeatureAnnotationService_RemoveTag_FullMethodName                        = "/dictybase.feature_annotation.FeatureAnnotationService/RemoveTag"
	FeatureAnnotationService_RemoveTags_FullMethodName                       = "/dictybase.feature_annotation.FeatureAnnotationService/RemoveTags"
	FeatureAnnotationService_ListFeatureAnnotationsByPubmedId_FullMethodName = "/dictybase.feature_annotation.FeatureAnnotationService/ListFeatureAnnotationsByPubmedId"
	FeatureAnnotationService_ListFeatureAnnotationsByDOI_FullMethodName      = "/dictybase.feature_annotation.FeatureAnnotationService/ListFeatureAnnotationsByDOI"
)
type FeatureAnnotationServiceClient interface {
	CreateFeatureAnnotation(ctx context.Context, in *NewFeatureAnnotation, opts ...grpc.CallOption) (*FeatureAnnotation, error)
	GetFeatureAnnotation(ctx context.Context, in *FeatureAnnotationId, opts ...grpc.CallOption) (*FeatureAnnotation, error)
	GetFeatureAnnotationByName(ctx context.Context, in *FeatureName, opts ...grpc.CallOption) (*FeatureAnnotation, error)
	UpdateFeatureAnnotation(ctx context.Context, in *FeatureAnnotationUpdate, opts ...grpc.CallOption) (*FeatureAnnotation, error)
	DeleteFeatureAnnotation(ctx context.Context, in *DeleteFeatureAnnotationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	AddTag(ctx context.Context, in *AddTagRequest, opts ...grpc.CallOption) (*FeatureAnnotation, error)
	AddTags(ctx context.Context, in *AddTagsRequest, opts ...grpc.CallOption) (*FeatureAnnotation, error)
	SetTags(ctx context.Context, in *SetTagsRequest, opts ...grpc.CallOption) (*FeatureAnnotation, error)
	UpdateTag(ctx context.Context, in *UpdateTagRequest, opts ...grpc.CallOption) (*FeatureAnnotation, error)
	RemoveTag(ctx context.Context, in *RemoveTagRequest, opts ...grpc.CallOption) (*FeatureAnnotation, error)
	RemoveTags(ctx context.Context, in *RemoveTagsRequest, opts ...grpc.CallOption) (*FeatureAnnotation, error)
	ListFeatureAnnotationsByPubmedId(ctx context.Context, in *PubmedId, opts ...grpc.CallOption) (*FeatureAnnotationCollection, error)
	ListFeatureAnnotationsByDOI(ctx context.Context, in *DOI, opts ...grpc.CallOption) (*FeatureAnnotationCollection, error)
}
type featureAnnotationServiceClient struct {
	cc grpc.ClientConnInterface
}
func NewFeatureAnnotationServiceClient(cc grpc.ClientConnInterface) FeatureAnnotationServiceClient
func (c *featureAnnotationServiceClient) CreateFeatureAnnotation(ctx context.Context, in *NewFeatureAnnotation, opts ...grpc.CallOption) (*FeatureAnnotation, error)
func (c *featureAnnotationServiceClient) GetFeatureAnnotation(ctx context.Context, in *FeatureAnnotationId, opts ...grpc.CallOption) (*FeatureAnnotation, error)
func (c *featureAnnotationServiceClient) GetFeatureAnnotationByName(ctx context.Context, in *FeatureName, opts ...grpc.CallOption) (*FeatureAnnotation, error)
func (c *featureAnnotationServiceClient) UpdateFeatureAnnotation(ctx context.Context, in *FeatureAnnotationUpdate, opts ...grpc.CallOption) (*FeatureAnnotation, error)
func (c *featureAnnotationServiceClient) DeleteFeatureAnnotation(ctx context.Context, in *DeleteFeatureAnnotationRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
func (c *featureAnnotationServiceClient) AddTag(ctx context.Context, in *AddTagRequest, opts ...grpc.CallOption) (*FeatureAnnotation, error)
func (c *featureAnnotationServiceClient) AddTags(ctx context.Context, in *AddTagsRequest, opts ...grpc.CallOption) (*FeatureAnnotation, error)
func (c *featureAnnotationServiceClient) SetTags(ctx context.Context, in *SetTagsRequest, opts ...grpc.CallOption) (*FeatureAnnotation, error)
func (c *featureAnnotationServiceClient) UpdateTag(ctx context.Context, in *UpdateTagRequest, opts ...grpc.CallOption) (*FeatureAnnotation, error)
func (c *featureAnnotationServiceClient) RemoveTag(ctx context.Context, in *RemoveTagRequest, opts ...grpc.CallOption) (*FeatureAnnotation, error)
func (c *featureAnnotationServiceClient) RemoveTags(ctx context.Context, in *RemoveTagsRequest, opts ...grpc.CallOption) (*FeatureAnnotation, error)
func (c *featureAnnotationServiceClient) ListFeatureAnnotationsByPubmedId(ctx context.Context, in *PubmedId, opts ...grpc.CallOption) (*FeatureAnnotationCollection, error)
func (c *featureAnnotationServiceClient) ListFeatureAnnotationsByDOI(ctx context.Context, in *DOI, opts ...grpc.CallOption) (*FeatureAnnotationCollection, error)
type FeatureAnnotationServiceServer interface {
	CreateFeatureAnnotation(context.Context, *NewFeatureAnnotation) (*FeatureAnnotation, error)
	GetFeatureAnnotation(context.Context, *FeatureAnnotationId) (*FeatureAnnotation, error)
	GetFeatureAnnotationByName(context.Context, *FeatureName) (*FeatureAnnotation, error)
	UpdateFeatureAnnotation(context.Context, *FeatureAnnotationUpdate) (*FeatureAnnotation, error)
	DeleteFeatureAnnotation(context.Context, *DeleteFeatureAnnotationRequest) (*emptypb.Empty, error)
	AddTag(context.Context, *AddTagRequest) (*FeatureAnnotation, error)
	AddTags(context.Context, *AddTagsRequest) (*FeatureAnnotation, error)
	SetTags(context.Context, *SetTagsRequest) (*FeatureAnnotation, error)
	UpdateTag(context.Context, *UpdateTagRequest) (*FeatureAnnotation, error)
	RemoveTag(context.Context, *RemoveTagRequest) (*FeatureAnnotation, error)
	RemoveTags(context.Context, *RemoveTagsRequest) (*FeatureAnnotation, error)
	ListFeatureAnnotationsByPubmedId(context.Context, *PubmedId) (*FeatureAnnotationCollection, error)
	ListFeatureAnnotationsByDOI(context.Context, *DOI) (*FeatureAnnotationCollection, error)
	mustEmbedUnimplementedFeatureAnnotationServiceServer()
}
type UnimplementedFeatureAnnotationServiceServer struct {
}
⋮----
func (UnimplementedFeatureAnnotationServiceServer) mustEmbedUnimplementedFeatureAnnotationServiceServer()
type UnsafeFeatureAnnotationServiceServer interface {
	mustEmbedUnimplementedFeatureAnnotationServiceServer()
}
func RegisterFeatureAnnotationServiceServer(s grpc.ServiceRegistrar, srv FeatureAnnotationServiceServer)
func _FeatureAnnotationService_CreateFeatureAnnotation_Handler(srv interface
func _FeatureAnnotationService_GetFeatureAnnotation_Handler(srv interface
func _FeatureAnnotationService_GetFeatureAnnotationByName_Handler(srv interface
func _FeatureAnnotationService_UpdateFeatureAnnotation_Handler(srv interface
func _FeatureAnnotationService_DeleteFeatureAnnotation_Handler(srv interface
func _FeatureAnnotationService_AddTag_Handler(srv interface
func _FeatureAnnotationService_AddTags_Handler(srv interface
func _FeatureAnnotationService_SetTags_Handler(srv interface
func _FeatureAnnotationService_UpdateTag_Handler(srv interface
func _FeatureAnnotationService_RemoveTag_Handler(srv interface
func _FeatureAnnotationService_RemoveTags_Handler(srv interface
func _FeatureAnnotationService_ListFeatureAnnotationsByPubmedId_Handler(srv interface
func _FeatureAnnotationService_ListFeatureAnnotationsByDOI_Handler(srv interface
var FeatureAnnotationService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "dictybase.feature_annotation.FeatureAnnotationService",
	HandlerType: (*FeatureAnnotationServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateFeatureAnnotation",
			Handler:    _FeatureAnnotationService_CreateFeatureAnnotation_Handler,
		},
		{
			MethodName: "GetFeatureAnnotation",
			Handler:    _FeatureAnnotationService_GetFeatureAnnotation_Handler,
		},
		{
			MethodName: "GetFeatureAnnotationByName",
			Handler:    _FeatureAnnotationService_GetFeatureAnnotationByName_Handler,
		},
		{
			MethodName: "UpdateFeatureAnnotation",
			Handler:    _FeatureAnnotationService_UpdateFeatureAnnotation_Handler,
		},
		{
			MethodName: "DeleteFeatureAnnotation",
			Handler:    _FeatureAnnotationService_DeleteFeatureAnnotation_Handler,
		},
		{
			MethodName: "AddTag",
			Handler:    _FeatureAnnotationService_AddTag_Handler,
		},
		{
			MethodName: "AddTags",
			Handler:    _FeatureAnnotationService_AddTags_Handler,
		},
		{
			MethodName: "SetTags",
			Handler:    _FeatureAnnotationService_SetTags_Handler,
		},
		{
			MethodName: "UpdateTag",
			Handler:    _FeatureAnnotationService_UpdateTag_Handler,
		},
		{
			MethodName: "RemoveTag",
			Handler:    _FeatureAnnotationService_RemoveTag_Handler,
		},
		{
			MethodName: "RemoveTags",
			Handler:    _FeatureAnnotationService_RemoveTags_Handler,
		},
		{
			MethodName: "ListFeatureAnnotationsByPubmedId",
			Handler:    _FeatureAnnotationService_ListFeatureAnnotationsByPubmedId_Handler,
		},
		{
			MethodName: "ListFeatureAnnotationsByDOI",
			Handler:    _FeatureAnnotationService_ListFeatureAnnotationsByDOI_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "dictybase/feature_annotation/feature_annotation.proto",
}
```

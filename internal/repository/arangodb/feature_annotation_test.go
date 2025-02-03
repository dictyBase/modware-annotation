package arangodb

import (
	"testing"
	"time"

	feature "github.com/dictyBase/go-genproto/dictybaseapis/feature_annotation"
	"github.com/dictyBase/modware-annotation/internal/repository"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGetFeatureAnnotation(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })

	// Add test feature annotation first
	baseDoc := &feature.NewFeatureAnnotation{
		Id:        "DDB_G0285425",
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
		Attributes: &feature.FeatureAnnotationAttributes{
			Name:         "gene name",
			Synonyms:     []string{"synonym1", "synonym2"},
			Publications: []string{"pub1", "pub2"},
			Pubmed:       []string{"123", "456"},
			Dblinks: []*feature.DbLink{
				{
					PrimaryId: "DDB_G0285425",
					Database:  "dictyBase",
					Version:   1,
					Linktype:  "gene",
					Url:       "http://dictybase.org/gene/DDB_G0285425",
					Label:     "gene page",
				},
			},
			Properties: []*feature.TagProperty{
				{
					Tag:   "description",
					Value: "test gene",
				},
			},
		},
	}
	added, err := repo.AddFeatureAnnotation(baseDoc)
	asrt.NoError(err, "expected no error adding test feature annotation")

	got, err := repo.GetFeatureAnnotation(added.AnnoId)
	asrt.NoError(err, "expected no error getting feature annotation")
	validateFeatureAnnotation(validateFeatureAnnotationParams{
		t:          t,
		assertions: asrt,
		got:        got,
		base:       baseDoc,
		key:        added.Key,
	})

	_, err = repo.GetFeatureAnnotation("non_existent_id")
	asrt.Error(err, "expected error for non-existent feature annotation")
	asrt.True(
		repository.IsAnnotationNotFound(err),
		"should be annotation not found error",
	)
}

func TestAddFeatureAnnotation(t *testing.T) {
	t.Parallel()
	baseDoc := &feature.NewFeatureAnnotation{
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
	}
	for _, tcs := range getFeatureTestCases() {
		t.Run(tcs.name, func(t *testing.T) {
			t.Parallel()
			asrt, repo := setUpFeatureTest(t)
			t.Cleanup(func() { _ = repo.Dbh().Drop() })
			baseDoc.Attributes = tcs.attrs
			baseDoc.Id = tcs.id
			doc, err := repo.AddFeatureAnnotation(baseDoc)
			if tcs.wantErr {
				asrt.Error(err, "expected error adding feature annotation")

				return
			}
			asrt.NoError(err, "expected no error adding feature annotation")
			validateFeatureAnnotation(validateFeatureAnnotationParams{
				t:          t,
				assertions: asrt,
				got:        doc,
				base:       baseDoc,
				key:        doc.Key,
			})
		})
	}
}

func TestAddDuplicateFeatureAnnotation(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })

	// Create base feature annotation
	baseDoc := &feature.NewFeatureAnnotation{
		Id:        "DDB_G0285425",
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
		Attributes: &feature.FeatureAnnotationAttributes{
			Name: "gene name",
		},
	}

	// Add first feature annotation
	_, err := repo.AddFeatureAnnotation(baseDoc)
	asrt.NoError(err, "expected no error adding first feature annotation")

	// Attempt to add duplicate feature annotation
	_, err = repo.AddFeatureAnnotation(baseDoc)
	asrt.Error(err, "expected error when adding duplicate feature annotation")
	asrt.Contains(
		err.Error(),
		"unique constraint violated",
		"expected duplicate error message",
	)
}

func TestRemoveFeatureAnnotation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		purge   bool
		wantErr bool
	}{
		{
			name:    "should soft delete feature annotation",
			purge:   false,
			wantErr: false,
		},
		{
			name:    "should purge feature annotation",
			purge:   true,
			wantErr: false,
		},
		{
			name:    "should return error for non-existent ID",
			purge:   false,
			wantErr: true,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			assert, repo := setUpFeatureTest(t)
			t.Cleanup(func() { _ = repo.Dbh().Drop() })
			identifier := getTestIdentifier(testCase.wantErr, repo, assert)
			err := repo.RemoveFeatureAnnotation(identifier, testCase.purge)
			if testCase.wantErr {
				assert.Error(
					err,
					"expected error removing non-existent feature annotation",
				)

				return
			}
			assert.NoError(err, "expected no error removing feature annotation")
			verifyRemoval(identifier, repo, assert)
		})
	}
}

func TestEditFeatureAnnotation(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })
	baseDoc := getBaseFeatureDoc()
	added, err := repo.AddFeatureAnnotation(baseDoc)
	asrt.NoError(err, "expected no error adding initial feature annotation")

	for _, tcs := range getEditFeatureTestCases(added.AnnoId) {
		t.Run(tcs.name, func(t *testing.T) {
			t.Parallel()
			doc, err := repo.EditFeatureAnnotation(tcs.update)
			if tcs.wantErr {
				verifyEditError(t, asrt, err)

				return
			}
			verifyEditSuccess(verifyEditSuccessParams{
				t:       t,
				asrt:    asrt,
				tce:     tcs,
				doc:     doc,
				baseDoc: baseDoc,
				added:   added,
			})
		})
	}
}

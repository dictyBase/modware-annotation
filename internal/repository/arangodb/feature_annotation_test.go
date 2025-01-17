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

	got, err := repo.GetFeatureAnnotation(added.Id)
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
				asrt.Error(err)

				return
			}
			asrt.NoError(err)
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

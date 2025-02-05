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
	feat := getCompleteFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "expected no error adding test feature annotation")

	got, err := repo.GetFeatureAnnotation(added.AnnoId)
	asrt.NoError(err, "expected no error getting feature annotation")
	validateBasicFields(validateFeatureAnnotationParams{
		t:          t,
		assertions: asrt,
		got:        got,
		base:       feat,
	})
	validateDbLinks(validateDbLinksParams{
		t:          t,
		assertions: asrt,
		got:        got.DbLinks,
		expected:   feat.Attributes.Dblinks,
	})
	validateProperties(validatePropertiesParams{
		t:          t,
		assertions: asrt,
		got:        got.Properties,
		expected:   feat.Attributes.Properties,
	})

	_, err = repo.GetFeatureAnnotation("non_existent_id")
	asrt.Error(err, "expected error for non-existent feature annotation")
	asrt.True(
		repository.IsAnnotationNotFound(err),
		"should be annotation not found error",
	)
}

func TestAddFeatureAnnotationBasic(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })
	baseFeat := &feature.NewFeatureAnnotation{
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
	}
	for _, nfeat := range getBasicTestCases() {
		baseFeat.Attributes = nfeat.Attributes
		baseFeat.Id = nfeat.Id
		doc, err := repo.AddFeatureAnnotation(baseFeat)
		asrt.NoError(err, "expected no error adding feature annotation")
		validateBasicFields(validateFeatureAnnotationParams{
			t:          t,
			assertions: asrt,
			got:        doc,
			base:       baseFeat,
		})
	}
}

func TestAddFeatureAnnotationFull(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })
	feat := getCombinedFeatureDoc(getBaseFeatureDoc, getMultiPropertyTestCase)
	doc, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "expected no error adding feature annotation")
	validateBasicFields(validateFeatureAnnotationParams{
		t:          t,
		assertions: asrt,
		got:        doc,
		base:       feat,
	})
	validateDbLinks(validateDbLinksParams{
		t:          t,
		assertions: asrt,
		got:        doc.DbLinks,
		expected:   feat.Attributes.Dblinks,
	})
	validateProperties(validatePropertiesParams{
		t:          t,
		assertions: asrt,
		got:        doc.Properties,
		expected:   feat.Attributes.Properties,
	})
}

func TestAddFeatureAnnotationMultiProperty(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })
	feat := getCombinedFeatureDoc(getBaseFeatureDoc, getMultiPropertyTestCase)
	doc, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "expected no error adding feature annotation")
	validateBasicFields(validateFeatureAnnotationParams{
		t:          t,
		assertions: asrt,
		got:        doc,
		base:       feat,
	})
	validateProperties(validatePropertiesParams{
		t:          t,
		assertions: asrt,
		got:        doc.Properties,
		expected:   feat.Attributes.Properties,
	})
}

func TestAddDuplicateFeatureAnnotation(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })

	// Create base feature annotation
	feat := &feature.NewFeatureAnnotation{
		Id:        "DDB_G0285425",
		CreatedBy: "mock@email.com",
		CreatedAt: timestamppb.New(time.Now()),
		Attributes: &feature.FeatureAnnotationAttributes{
			Name: "gene name",
		},
	}

	// Add first feature annotation
	_, err := repo.AddFeatureAnnotation(feat)
	asrt.NoError(err, "expected no error adding first feature annotation")

	// Attempt to add duplicate feature annotation
	_, err = repo.AddFeatureAnnotation(feat)
	asrt.Error(err, "expected error when adding duplicate feature annotation")
	asrt.Contains(
		err.Error(),
		"unique constraint violated",
		"expected duplicate error message",
	)
}

func TestGodaRemoveFeatureAnnotation(t *testing.T) {
	t.Parallel()
	for _, testCase := range getRemoveTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var identifier string
			assert, repo := setUpFeatureTest(t)
			t.Cleanup(func() { _ = repo.Dbh().Drop() })
			if testCase.wantErr {
				identifier = "non_existent_id"
			} else {
				doc, err := repo.AddFeatureAnnotation(getFullFeatureDoc())
				assert.NoError(err, "expected no error adding test feature annotation")
				identifier = doc.AnnoId
			}
			err := repo.RemoveFeatureAnnotation(identifier, testCase.purge)
			if testCase.wantErr {
				assert.Error(
					err,
					"expected error removing non-existent feature annotation",
				)

				return
			}
			assert.NoError(err, "expected no error removing feature annotation")
			_, err = repo.GetFeatureAnnotation(identifier)
			assert.Error(
				err,
				"expected error getting removed feature annotation",
			)
			assert.True(
				repository.IsAnnotationNotFound(err),
				"should be annotation not found error",
			)
		})
	}
}

func TestEditFeatureAnnotation(t *testing.T) {
	t.Parallel()
	asrt, repo := setUpFeatureTest(t)
	t.Cleanup(func() { _ = repo.Dbh().Drop() })
	feat := getFullFeatureDoc()
	added, err := repo.AddFeatureAnnotation(feat)
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
				baseDoc: feat,
				added:   added,
			})
		})
	}
}

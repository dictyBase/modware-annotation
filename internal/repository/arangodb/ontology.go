package arangodb

import (
	"context"
	"fmt"
	"io"

	A "github.com/IBM/fp-go/v2/array"
	E "github.com/IBM/fp-go/v2/either"
	fperrors "github.com/IBM/fp-go/v2/errors"
	F "github.com/IBM/fp-go/v2/function"
	IOE "github.com/IBM/fp-go/v2/ioeither"
	L "github.com/IBM/fp-go/v2/optics/lens"
	ioeutils "github.com/dictyBase/fp-go-loom/ioeitherutils"
	predarrays "github.com/dictyBase/fp-go-loom/predicate/array"
	"github.com/dictyBase/go-obograph/storage"
	ontoarango "github.com/dictyBase/go-obograph/storage/arangodb"
	"github.com/dictyBase/modware-annotation/internal/model"
	"github.com/dictyBase/modware-annotation/internal/repository"
)

// resolvedTag is one term returned by tagResolveQ.
type resolvedTag struct {
	*model.AnnoTag

	// Key is the cvterm document _id, used as the edge target of an annotation.
	Key string `json:"key"`
}

// Values of the include_obsolete bind parameter of tagResolveQ.
const (
	activeTermsOnly   = false
	withObsoleteTerms = true
)

var (
	runTagResolveQueryFault = fperrors.OnError(
		"error in running tag resolution query",
	)
	readTagResolveRowFault = fperrors.OnError(
		"error in reading tag resolution result",
	)
	resolvedTagKeyLens = L.MakeLens(
		func(rt resolvedTag) string { return rt.Key },
		func(rt resolvedTag, key string) resolvedTag {
			rt.Key = key

			return rt
		},
	)
	resolvedAnnoTagLens = L.MakeLens(
		func(rt resolvedTag) *model.AnnoTag { return rt.AnnoTag },
		func(rt resolvedTag, tag *model.AnnoTag) resolvedTag {
			rt.AnnoTag = tag

			return rt
		},
	)
)

func (ar *arangorepository) LoadOboJSON(rde io.Reader) (*storage.UploadInformation, error) {
	dsb, err := ontoarango.NewDataSourceFromDb(ar.database, &ontoarango.CollectionParams{
		OboGraph:     ar.onto.Obog.Name(),
		GraphInfo:    ar.onto.Cv.Name(),
		Relationship: ar.onto.Rel.Name(),
		Term:         ar.onto.Term.Name(),
	})
	if err != nil {
		return &storage.UploadInformation{}, fmt.Errorf(
			"error in creating new data source %s",
			err,
		)
	}

	info, err := storage.LoadOboJSONFromDataSource(rde, dsb)
	if err != nil {
		return &storage.UploadInformation{}, fmt.Errorf(
			"error in uploading JSON %s",
			err,
		)
	}

	return info, nil
}

func readResolvedTagsRow(
	row *contextResult,
) IOE.IOEither[error, []resolvedTag] {
	return F.Pipe1(
		IOE.TryCatchError(func() ([]resolvedTag, error) {
			var tags []resolvedTag
			err := row.Read(&tags)

			return tags, err
		}),
		IOE.MapLeft[[]resolvedTag](readTagResolveRowFault),
	)
}

// resolveTag resolves tag in onto with the rules of tagResolveQ. It accepts
// exactly one term. No term is an AnnoTagNotFoundError and more than one is
// an AnnoTagAmbiguousError, so a shared synonym never silently picks a term.
func (ar *arangorepository) resolveTag(
	ctx context.Context,
	onto, tag string,
	includeObsolete bool,
) IOE.IOEither[error, resolvedTag] {
	notFound := repository.NewAnnoTagNotFoundError(tag, onto)
	ambiguous := repository.NewAnnoTagAmbiguousError(tag, onto)

	return F.Pipe5(
		IOE.TryCatchError(func() (*contextResult, error) {
			return ar.getRow(ctx, tagResolveQ, map[string]any{
				cvCollectionBind:     ar.onto.Cv.Name(),
				cvtermCollectionBind: ar.onto.Term.Name(),
				"ontology":           onto,
				"tag":                tag,
				"include_obsolete":   includeObsolete,
			})
		}),
		IOE.MapLeft[*contextResult](runTagResolveQueryFault),
		IOE.Chain(readResolvedTagsRow),
		IOE.ChainEitherK(E.FromPredicate(
			predarrays.MaxLen[resolvedTag](1),
			F.Constant1[[]resolvedTag](ambiguous),
		)),
		IOE.Map[error](A.Head[resolvedTag]),
		IOE.ChainEitherK(E.FromOption[resolvedTag](F.Constant(notFound))),
	)
}

// lookupTermID resolves term in onto to the cvterm _id of one active term.
func (ar *arangorepository) lookupTermID(
	ctx context.Context,
	onto, term string,
) IOE.IOEither[error, string] {
	return F.Pipe1(
		ar.resolveTag(ctx, onto, term, activeTermsOnly),
		IOE.Map[error](resolvedTagKeyLens.Get),
	)
}

// GetAnnotationTag retrieves one tag of ontology. The tag can be a term id
// (PREFIX:digits or PREFIX_digits), an exact label, or a synonym that only
// one term has. Obsolete terms are returned with IsObsolete set.
func (ar *arangorepository) GetAnnotationTag(
	tag, ontology string,
) (*model.AnnoTag, error) {
	ctx := context.Background()
	tagResult := F.Pipe2(
		ar.resolveTag(ctx, ontology, tag, withObsoleteTerms),
		IOE.Map[error](resolvedAnnoTagLens.Get),
		ioeutils.ToEither[error, *model.AnnoTag],
	)

	return E.UnwrapError(tagResult)
}

func (ar *arangorepository) termID(onto, term string) (string, error) {
	ctx := context.Background()
	termIDResult := F.Pipe1(
		ar.lookupTermID(ctx, onto, term),
		ioeutils.ToEither[error, string],
	)

	return E.UnwrapError(termIDResult)
}

func (ar *arangorepository) termName(tid string) (string, error) {
	var name string
	cvtr, err := ar.database.GetRow(cvtID2LblQ, map[string]any{
		cvtermCollectionBind: ar.onto.Term.Name(),
		"id":                 tid,
	})
	if err != nil {
		return name,
			fmt.Errorf("error in running tag retrieving query %s", err)
	}
	if cvtr.IsEmpty() {
		return name, fmt.Errorf("cvterm id %s does not exist", tid)
	}
	if err := cvtr.Read(&name); err != nil {
		return name, fmt.Errorf("error in retrieving tag %s", err)
	}

	return name, nil
}

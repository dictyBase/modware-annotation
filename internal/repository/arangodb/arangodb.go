// Package arangodb implements the annotation repository using ArangoDB.
package arangodb

import (
	"context"
	"fmt"

	E "github.com/IBM/fp-go/v2/either"
	F "github.com/IBM/fp-go/v2/function"
	IOE "github.com/IBM/fp-go/v2/ioeither"
	L "github.com/IBM/fp-go/v2/optics/lens"
	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	ioeutils "github.com/dictyBase/fp-go-loom/ioeitherutils"
	ontoarango "github.com/dictyBase/go-obograph/storage/arangodb"
	repo "github.com/dictyBase/modware-annotation/internal/repository"
	"github.com/go-playground/validator/v10"
)

type annoc struct {
	annot  driver.Collection
	term   driver.Collection
	ver    driver.Collection
	annog  driver.Collection
	verg   driver.Graph
	annotg driver.Graph
}

type arangorepository struct {
	connP    *manager.ConnectParams
	sess     *manager.Session
	database *manager.Database
	anno     *annoc
	onto     *ontoarango.OntoCollection
}

// sessionHandles holds the connection handle pair produced by one call to
// manager.NewSessionDb, so the session and its database are created together
// and never diverge.
type sessionHandles struct {
	sess *manager.Session
	dbh  *manager.Database
}

// annotationRepoState carries the dependencies and the intermediates of the
// annotation repository constructor pipeline. Every field a step fills has a
// lens, so the whole setup composes through IOE.Bind and is forced once.
type annotationRepoState struct {
	connP      *manager.ConnectParams
	collP      *CollectionParams
	ontoP      *ontoarango.CollectionParams
	handles    sessionHandles
	db         driver.Database
	onto       *ontoarango.OntoCollection
	anno       *annoc
	viewExists bool
	view       driver.View
	searchView driver.ArangoSearchView
	viewProps  driver.ArangoSearchViewProperties
}

// documentCollectionState carries the values of the annotation document
// collection setup pipeline.
type documentCollectionState struct {
	dbh   *manager.Database
	collP *CollectionParams
	annot driver.Collection
	annog driver.Collection
	term  driver.Collection
	ver   driver.Collection
}

// annotationCollectionState carries the values of the annotation collection,
// graph, and index setup pipeline.
type annotationCollectionState struct {
	dbh    *manager.Database
	collP  *CollectionParams
	onto   *ontoarango.OntoCollection
	anno   *annoc
	verg   driver.Graph
	annotg driver.Graph
}

// taggedAnnotationRepoResult is the single value the constructor pipeline
// folds into at its (repository, error) boundary.
type taggedAnnotationRepoResult struct {
	repo repo.TaggedAnnotationRepository
	err  error
}

var (
	handlesLens = L.MakeLens(
		func(s annotationRepoState) sessionHandles { return s.handles },
		func(s annotationRepoState, v sessionHandles) annotationRepoState {
			s.handles = v
			return s
		},
	)
	dbLens = L.MakeLens(
		func(s annotationRepoState) driver.Database { return s.db },
		func(s annotationRepoState, v driver.Database) annotationRepoState {
			s.db = v
			return s
		},
	)
	ontoLens = L.MakeLens(
		func(s annotationRepoState) *ontoarango.OntoCollection { return s.onto },
		func(
			s annotationRepoState,
			v *ontoarango.OntoCollection,
		) annotationRepoState {
			s.onto = v
			return s
		},
	)
	annoLens = L.MakeLens(
		func(s annotationRepoState) *annoc { return s.anno },
		func(s annotationRepoState, v *annoc) annotationRepoState {
			s.anno = v
			return s
		},
	)
	viewExistsLens = L.MakeLens(
		func(s annotationRepoState) bool { return s.viewExists },
		func(s annotationRepoState, v bool) annotationRepoState {
			s.viewExists = v
			return s
		},
	)
	viewLens = L.MakeLens(
		func(s annotationRepoState) driver.View { return s.view },
		func(s annotationRepoState, v driver.View) annotationRepoState {
			s.view = v
			return s
		},
	)
	searchViewLens = L.MakeLens(
		func(s annotationRepoState) driver.ArangoSearchView {
			return s.searchView
		},
		func(
			s annotationRepoState,
			v driver.ArangoSearchView,
		) annotationRepoState {
			s.searchView = v
			return s
		},
	)
	viewPropsLens = L.MakeLens(
		func(s annotationRepoState) driver.ArangoSearchViewProperties {
			return s.viewProps
		},
		func(
			s annotationRepoState,
			v driver.ArangoSearchViewProperties,
		) annotationRepoState {
			s.viewProps = v
			return s
		},
	)
	docAnnotLens = L.MakeLens(
		func(s documentCollectionState) driver.Collection { return s.annot },
		func(s documentCollectionState, v driver.Collection) documentCollectionState {
			s.annot = v
			return s
		},
	)
	docAnnogLens = L.MakeLens(
		func(s documentCollectionState) driver.Collection { return s.annog },
		func(s documentCollectionState, v driver.Collection) documentCollectionState {
			s.annog = v
			return s
		},
	)
	docTermLens = L.MakeLens(
		func(s documentCollectionState) driver.Collection { return s.term },
		func(s documentCollectionState, v driver.Collection) documentCollectionState {
			s.term = v
			return s
		},
	)
	docVerLens = L.MakeLens(
		func(s documentCollectionState) driver.Collection { return s.ver },
		func(s documentCollectionState, v driver.Collection) documentCollectionState {
			s.ver = v
			return s
		},
	)
	collAnnoLens = L.MakeLens(
		func(s annotationCollectionState) *annoc { return s.anno },
		func(s annotationCollectionState, v *annoc) annotationCollectionState {
			s.anno = v
			return s
		},
	)
	collVergLens = L.MakeLens(
		func(s annotationCollectionState) driver.Graph { return s.verg },
		func(s annotationCollectionState, v driver.Graph) annotationCollectionState {
			s.verg = v
			return s
		},
	)
	collAnnotgLens = L.MakeLens(
		func(s annotationCollectionState) driver.Graph { return s.annotg },
		func(s annotationCollectionState, v driver.Graph) annotationCollectionState {
			s.annotg = v
			return s
		},
	)
)

// NewTaggedAnnotationRepo creates the annotation repository together with its
// ArangoSearch assets. The whole setup is one uninterrupted IOEither pipeline
// forced exactly once, at the (repository, error) boundary of this function.
func NewTaggedAnnotationRepo(
	connP *manager.ConnectParams,
	collP *CollectionParams,
	ontoP *ontoarango.CollectionParams,
) (repo.TaggedAnnotationRepository, error) {
	placeholder := &arangorepository{}
	onLeft := failedAnnotationRepo(placeholder)

	result := F.Pipe10(
		annotationRepoState{connP: connP, collP: collP, ontoP: ontoP},
		validatedAnnotationRepoState,
		IOE.Bind(handlesLens.Set, openAnnotationSession),
		IOE.Bind(dbLens.Set, annotationDriverDatabase),
		IOE.Bind(ontoLens.Set, createOntologyCollection),
		IOE.Bind(annoLens.Set, setAnnotationCollection),
		IOE.ChainFirst(ensureAutocompleteSearch),
		IOE.ChainFirst(ensureFullSearch),
		IOE.Map[error](toTaggedAnnotationRepo),
		ioeutils.ToEither[error, repo.TaggedAnnotationRepository],
		E.Fold(onLeft, successfulAnnotationRepo),
	)

	return result.repo, result.err
}

// validatedAnnotationRepoState rejects unusable collection parameters before
// any connection is opened.
func validatedAnnotationRepoState(
	st annotationRepoState,
) IOE.IOEither[error, annotationRepoState] {
	validationErr := validator.New().Struct(st.collP)
	validation := F.Pipe1(
		E.TryCatchError(st, validationErr),
		E.MapLeft[annotationRepoState](func(err error) error {
			return fmt.Errorf("error in validation %s", err)
		}),
	)

	return IOE.FromEither(validation)
}

// openAnnotationSession opens the ArangoDB session and its database handle.
func openAnnotationSession(
	st annotationRepoState,
) IOE.IOEither[error, sessionHandles] {
	return F.Pipe1(
		IOE.TryCatchError(func() (sessionHandles, error) {
			return openSessionDB(st.connP)
		}),
		IOE.MapLeft[sessionHandles](func(err error) error {
			return fmt.Errorf("error in creating new session %s", err)
		}),
	)
}

// openSessionDB is the raw session call. The driver reports the session and its
// database together, so the raw shell pairs them and the pipeline step only
// lifts the result.
func openSessionDB(connP *manager.ConnectParams) (sessionHandles, error) {
	sess, dbh, err := manager.NewSessionDb(connP)

	return sessionHandles{sess: sess, dbh: dbh}, err
}

// annotationDriverDatabase exposes the driver database handle that carries
// the view and analyzer operations. It runs only after the session step
// succeeded, so the wrapped handle is always non-nil.
func annotationDriverDatabase(
	st annotationRepoState,
) IOE.IOEither[error, driver.Database] {
	handle := st.handles.dbh.Handler()

	return IOE.Of[error](handle)
}

// createOntologyCollection finds or creates the ontology collection set that
// the annotation tag graph points into.
func createOntologyCollection(
	st annotationRepoState,
) IOE.IOEither[error, *ontoarango.OntoCollection] {
	return F.Pipe1(
		IOE.TryCatchError(func() (*ontoarango.OntoCollection, error) {
			return ontoarango.CreateCollection(st.handles.dbh, st.ontoP)
		}),
		IOE.MapLeft[*ontoarango.OntoCollection](func(err error) error {
			return fmt.Errorf("error in creating ontology collection %s", err)
		}),
	)
}

// setAnnotationCollection creates the annotation documents, the two graphs
// over them, and the annotation indexes, and returns the assembled bundle.
func setAnnotationCollection(
	st annotationRepoState,
) IOE.IOEither[error, *annoc] {
	return F.Pipe5(
		IOE.Of[error](annotationCollectionState{
			dbh:   st.handles.dbh,
			collP: st.collP,
			onto:  st.onto,
		}),
		IOE.Bind(collAnnoLens.Set, setDocumentCollection),
		IOE.Bind(collVergLens.Set, createAnnotationVersionGraph),
		IOE.Bind(collAnnotgLens.Set, createAnnotationTagGraph),
		IOE.Chain(ensureAnnotationIndexes),
		IOE.Map[error](toAnnotationCollection),
	)
}

// setDocumentCollection finds or creates the four annotation collections and
// returns them as one bundle. Each collection is one raw operation inside its
// own pipeline step.
func setDocumentCollection(
	st annotationCollectionState,
) IOE.IOEither[error, *annoc] {
	return F.Pipe5(
		IOE.Of[error](documentCollectionState{dbh: st.dbh, collP: st.collP}),
		IOE.Bind(docAnnotLens.Set, findOrCreateAnnotationCollection),
		IOE.Bind(docAnnogLens.Set, findOrCreateAnnotationGroupCollection),
		IOE.Bind(docTermLens.Set, findOrCreateAnnotationTermCollection),
		IOE.Bind(docVerLens.Set, findOrCreateAnnotationVersionCollection),
		IOE.Map[error](toDocumentCollections),
	)
}

// findOrCreateAnnotationCollection finds or creates the annotation document
// collection.
func findOrCreateAnnotationCollection(
	st documentCollectionState,
) IOE.IOEither[error, driver.Collection] {
	return F.Pipe1(
		IOE.TryCatchError(func() (driver.Collection, error) {
			return st.dbh.FindOrCreateCollection(
				st.collP.Annotation,
				&driver.CreateCollectionOptions{},
			)
		}),
		IOE.MapLeft[driver.Collection](func(err error) error {
			return fmt.Errorf("error in finding or creating collection %s", err)
		}),
	)
}

// findOrCreateAnnotationGroupCollection finds or creates the annotation group
// document collection.
func findOrCreateAnnotationGroupCollection(
	st documentCollectionState,
) IOE.IOEither[error, driver.Collection] {
	return F.Pipe1(
		IOE.TryCatchError(func() (driver.Collection, error) {
			return st.dbh.FindOrCreateCollection(
				st.collP.AnnoGroup,
				&driver.CreateCollectionOptions{},
			)
		}),
		IOE.MapLeft[driver.Collection](func(err error) error {
			return fmt.Errorf("error in finding or creating collection %s", err)
		}),
	)
}

// findOrCreateAnnotationTermCollection finds or creates the annotation term
// edge collection.
func findOrCreateAnnotationTermCollection(
	st documentCollectionState,
) IOE.IOEither[error, driver.Collection] {
	return F.Pipe1(
		IOE.TryCatchError(func() (driver.Collection, error) {
			return st.dbh.FindOrCreateCollection(
				st.collP.AnnoTerm,
				&driver.CreateCollectionOptions{
					Type: driver.CollectionTypeEdge,
				},
			)
		}),
		IOE.MapLeft[driver.Collection](func(err error) error {
			return fmt.Errorf("error in finding or creating collection %s", err)
		}),
	)
}

// findOrCreateAnnotationVersionCollection finds or creates the annotation
// version edge collection.
func findOrCreateAnnotationVersionCollection(
	st documentCollectionState,
) IOE.IOEither[error, driver.Collection] {
	return F.Pipe1(
		IOE.TryCatchError(func() (driver.Collection, error) {
			return st.dbh.FindOrCreateCollection(
				st.collP.AnnoVersion,
				&driver.CreateCollectionOptions{
					Type: driver.CollectionTypeEdge,
				},
			)
		}),
		IOE.MapLeft[driver.Collection](func(err error) error {
			return fmt.Errorf("error in finding or creating collection %s", err)
		}),
	)
}

// toDocumentCollections assembles the four annotation collections into the
// bundle the rest of the setup pipeline threads.
func toDocumentCollections(st documentCollectionState) *annoc {
	return &annoc{
		annot: st.annot,
		annog: st.annog,
		term:  st.term,
		ver:   st.ver,
	}
}

// createAnnotationVersionGraph finds or creates the graph that links one
// annotation version to the next.
func createAnnotationVersionGraph(
	st annotationCollectionState,
) IOE.IOEither[error, driver.Graph] {
	edges := []driver.EdgeDefinition{
		{
			Collection: st.anno.ver.Name(),
			From:       []string{st.anno.annot.Name()},
			To:         []string{st.anno.annot.Name()},
		},
	}

	return F.Pipe1(
		IOE.TryCatchError(func() (driver.Graph, error) {
			return st.dbh.FindOrCreateGraph(st.collP.AnnoVerGraph, edges)
		}),
		IOE.MapLeft[driver.Graph](func(err error) error {
			return fmt.Errorf("error in creating graph %s", err)
		}),
	)
}

// createAnnotationTagGraph finds or creates the graph that links one
// annotation to its ontology term.
func createAnnotationTagGraph(
	st annotationCollectionState,
) IOE.IOEither[error, driver.Graph] {
	edges := []driver.EdgeDefinition{
		{
			Collection: st.anno.term.Name(),
			From:       []string{st.anno.annot.Name()},
			To:         []string{st.onto.Term.Name()},
		},
	}

	return F.Pipe1(
		IOE.TryCatchError(func() (driver.Graph, error) {
			return st.dbh.FindOrCreateGraph(st.collP.AnnoTagGraph, edges)
		}),
		IOE.MapLeft[driver.Graph](func(err error) error {
			return fmt.Errorf("error in creating graph %s", err)
		}),
	)
}

// ensureAnnotationIndexes creates the persistent indexes of the annotation
// collection. The step reports a flag that the pipeline does not need, so
// ChainFirst keeps the collection state flowing.
func ensureAnnotationIndexes(
	st annotationCollectionState,
) IOE.IOEither[error, annotationCollectionState] {
	return F.Pipe1(
		IOE.Of[error](st),
		IOE.ChainFirst(createAnnotationIndexesStep),
	)
}

// createAnnotationIndexesStep lifts the raw index call into the pipeline.
func createAnnotationIndexesStep(
	st annotationCollectionState,
) IOE.IOEither[error, bool] {
	return IOE.TryCatchError(func() (bool, error) {
		return createAnnotationIndexes(st)
	})
}

// createAnnotationIndexes is the raw index call. The driver reports three
// values, so the raw shell lives here and the pipeline step only lifts it.
func createAnnotationIndexes(st annotationCollectionState) (bool, error) {
	collName := st.anno.annot.Name()
	_, created, err := st.dbh.EnsurePersistentIndex(
		collName,
		st.collP.AnnoIndexes,
		&driver.EnsurePersistentIndexOptions{InBackground: true},
	)

	return created, err
}

// toAnnotationCollection assembles the collection bundle with its graphs, so
// callers that walk the graphs keep working.
func toAnnotationCollection(st annotationCollectionState) *annoc {
	return &annoc{
		annot:  st.anno.annot,
		annog:  st.anno.annog,
		term:   st.anno.term,
		ver:    st.anno.ver,
		verg:   st.verg,
		annotg: st.annotg,
	}
}

// toTaggedAnnotationRepo projects the completed constructor state onto the
// repository interface.
func toTaggedAnnotationRepo(
	st annotationRepoState,
) repo.TaggedAnnotationRepository {
	return &arangorepository{
		connP:    st.connP,
		sess:     st.handles.sess,
		database: st.handles.dbh,
		onto:     st.onto,
		anno:     st.anno,
	}
}

// successfulAnnotationRepo is the boundary projection of a completed
// constructor pipeline.
func successfulAnnotationRepo(
	r repo.TaggedAnnotationRepository,
) taggedAnnotationRepoResult {
	return taggedAnnotationRepoResult{repo: r}
}

// failedAnnotationRepo returns the boundary projection of a failed
// constructor pipeline: the placeholder repository and the error.
func failedAnnotationRepo(
	placeholder repo.TaggedAnnotationRepository,
) func(error) taggedAnnotationRepoResult {
	return func(err error) taggedAnnotationRepoResult {
		return taggedAnnotationRepoResult{repo: placeholder, err: err}
	}
}

// Clear clears all annotations and related ontologies from the repository
// datasource.
func (ar *arangorepository) Clear() error {
	ctx := context.Background()
	if err := ar.ClearAnnotations(); err != nil {
		return err
	}
	for _, c := range []driver.Collection{
		ar.onto.Term, ar.onto.Cv, ar.onto.Rel,
	} {
		if err := c.Truncate(ctx); err != nil {
			return fmt.Errorf("error in truncating %s", err)
		}
	}

	err := ar.onto.Obog.Remove(ctx)
	if err != nil {
		return fmt.Errorf("error in removing graph %s", err)
	}

	return nil
}

// ClearAnnotations clears all annotations from the repository datasource.
func (ar *arangorepository) ClearAnnotations() error {
	ctx := context.Background()
	for _, c := range []driver.Collection{
		ar.anno.annot, ar.anno.ver, ar.anno.term, ar.anno.annog,
	} {
		if err := c.Truncate(ctx); err != nil {
			return fmt.Errorf("error in truncating %s", err)
		}
	}
	for _, grph := range []driver.Graph{
		ar.anno.verg,
		ar.anno.annotg,
	} {
		arangoDB := ar.database.Handler()
		isok, err := arangoDB.GraphExists(ctx, grph.Name())
		if err != nil {
			return fmt.Errorf("error in checking existence of graph %s", err)
		}
		if !isok {
			continue
		}
		if err := grph.Remove(ctx); err != nil {
			return fmt.Errorf("error in removing graph %s", err)
		}
	}

	return nil
}

func DocumentsExists(c driver.Collection, ids ...string) error {
	ctx := context.Background()
	for _, kdi := range ids {
		ok, err := c.DocumentExists(ctx, kdi)
		if err != nil {
			return fmt.Errorf("error in checking for existence of identifier %s %s", kdi, err)
		}
		if !ok {
			return &repo.AnnoNotFoundError{ID: kdi}
		}
	}

	return nil
}

func (ar *arangorepository) Dbh() *manager.Database {
	return ar.database
}

package arangodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	A "github.com/IBM/fp-go/v2/array"
	F "github.com/IBM/fp-go/v2/function"
	IOE "github.com/IBM/fp-go/v2/ioeither"
	O "github.com/IBM/fp-go/v2/option"
	P "github.com/IBM/fp-go/v2/predicate"
	S "github.com/IBM/fp-go/v2/string"
	driver "github.com/arangodb/go-driver"
	manager "github.com/dictyBase/arangomanager"
	"github.com/dictyBase/modware-annotation/internal/model"
)

type (
	// dbRows is the read side of a multi-row query result. It is an interface so
	// the row scanner stays independent of the concrete ArangoDB result set.
	dbRows interface {
		IsEmpty() bool
		Scan() bool
		Read(iface any) error
		Close() error
	}

	// contextResultset wraps a driver.Cursor with the caller's context so
	// subsequent batch reads and document decoding honor cancellation and
	// deadlines.
	contextResultset struct {
		cursor driver.Cursor
		ctx    context.Context
		empty  bool
	}

	// contextResult wraps a single-row driver.Cursor with the caller's context.
	contextResult struct {
		cursor driver.Cursor
		ctx    context.Context
		empty  bool
	}

	// runningQueryEntry mirrors one item returned by ArangoDB's
	// /_api/query/current endpoint.
	runningQueryEntry struct {
		ID    string `json:"id"`
		Query string `json:"query"`
	}
)

const (
	// minQueryMaxRuntimeSeconds is the floor passed to driver.WithQueryMaxRuntime
	// when a context deadline has already expired or is sub-millisecond, because
	// go-driver omits a 0 maxRuntime (which ArangoDB treats as unlimited).
	minQueryMaxRuntimeSeconds = 0.001
	killQueryTimeout          = 2 * time.Second
	killQueryAttempts         = 5
	killQueryRetryInterval    = 20 * time.Millisecond
)

var (
	// querySeq generates monotonic markers for active AQL queries so a canceled
	// request can locate and terminate its in-flight server query.
	querySeq atomic.Uint64

	isNonNilContext = P.IsNonZero[context.Context]()
	hasContextError = F.Pipe1(
		P.IsNonZero[error](),
		P.ContraMap(context.Context.Err),
	)
	hasConnectParams = F.Pipe1(
		P.IsNonZero[*manager.ConnectParams](),
		P.ContraMap(repoConnectParams),
	)
	isTLSConnect = F.Pipe1(
		F.Identity[bool],
		P.ContraMap(connectParamsIsTLS),
	)
	selectScheme = F.Pipe1(
		isTLSConnect,
		P.Fold(
			F.Constant1[*manager.ConnectParams]("http"),
			F.Constant1[*manager.ConnectParams]("https"),
		),
	)
	hasMetaKey = F.Pipe1(
		S.IsNonEmpty,
		P.ContraMap(documentMetaKey),
	)
)

func repoConnectParams(ar *arangorepository) *manager.ConnectParams {
	return ar.connP
}

func connectParamsIsTLS(c *manager.ConnectParams) bool {
	return c.Istls
}

func documentMetaKey(m driver.DocumentMeta) string {
	return m.Key
}

func nextQueryMarker() string {
	seq := querySeq.Add(1)

	return fmt.Sprintf("/* modware-qid:%d-%d */ ", time.Now().UnixNano(), seq)
}

func normalizeQueryContext(ctx context.Context) context.Context {
	return F.Pipe2(
		ctx,
		O.FromPredicate(isNonNilContext),
		O.GetOrElse(context.Background),
	)
}

func deadlineRemainingSeconds(deadline time.Time) float64 {
	return max(time.Until(deadline).Seconds(), minQueryMaxRuntimeSeconds)
}

func applyMaxRuntime(baseCtx context.Context) func(float64) context.Context {
	return func(maxRuntime float64) context.Context {
		return driver.WithQueryMaxRuntime(baseCtx, maxRuntime)
	}
}

func queryContextWithMaxRuntime(ctx context.Context) context.Context {
	baseCtx := normalizeQueryContext(ctx)
	withRuntime := applyMaxRuntime(baseCtx)

	return F.Pipe3(
		baseCtx,
		O.FromValidation(context.Context.Deadline),
		O.Map(deadlineRemainingSeconds),
		O.Fold(F.Constant(baseCtx), withRuntime),
	)
}

func (ar *arangorepository) queryAPIBaseURL() string {
	scheme := F.Pipe1(ar.connP, selectScheme)
	dbName := ar.database.Handler().Name()

	return fmt.Sprintf(
		"%s://%s:%d/_db/%s/_api/query",
		scheme,
		ar.connP.Host,
		ar.connP.Port,
		dbName,
	)
}

func (ar *arangorepository) fetchRunningQueries(
	ctx context.Context,
) ([]runningQueryEntry, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		ar.queryAPIBaseURL()+"/current",
		nil,
	)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(ar.connP.User, ar.connP.Pass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var entries []runningQueryEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, err
	}

	return entries, nil
}

func (ar *arangorepository) deleteRunningQuery(
	ctx context.Context,
	queryID string,
) bool {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		ar.queryAPIBaseURL()+"/"+queryID,
		nil,
	)
	if err != nil {
		return false
	}
	req.SetBasicAuth(ar.connP.User, ar.connP.Pass)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	_ = resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

func hasQueryMarker(marker string) P.Predicate[runningQueryEntry] {
	return func(entry runningQueryEntry) bool {
		return strings.Contains(entry.Query, marker)
	}
}

func entryQueryID(entry runningQueryEntry) string {
	return entry.ID
}

func (ar *arangorepository) killRunningQueryOnce(
	ctx context.Context,
	marker string,
) bool {
	entries, err := ar.fetchRunningQueries(ctx)
	if err != nil {
		return false
	}
	matchMarker := hasQueryMarker(marker)
	deleteQuery := F.Bind1st(ar.deleteRunningQuery, ctx)

	return F.Pipe3(
		entries,
		A.FindFirst(matchMarker),
		O.Map(entryQueryID),
		O.Fold(F.Constant(false), deleteQuery),
	)
}

func (ar *arangorepository) killRunningQueryByMarker(marker string) {
	if !hasConnectParams(ar) {
		return
	}
	bgCtx := context.Background()
	ctx, cancel := context.WithTimeout(bgCtx, killQueryTimeout)
	defer cancel()
	for range killQueryAttempts {
		if ar.killRunningQueryOnce(ctx, marker) {
			return
		}
		time.Sleep(killQueryRetryInterval)
	}
}

func (ar *arangorepository) searchRows(
	ctx context.Context,
	query string,
	bindVars map[string]any,
) (dbRows, error) {
	baseCtx := normalizeQueryContext(ctx)
	marker := nextQueryMarker()
	taggedQuery := marker + query
	dbh := ar.database.Handler()
	if err := dbh.ValidateQuery(baseCtx, taggedQuery); err != nil {
		return &contextResultset{empty: true, ctx: baseCtx}, fmt.Errorf(
			"error in validating the query %w",
			err,
		)
	}
	queryCtx := queryContextWithMaxRuntime(baseCtx)
	cursor, err := dbh.Query(queryCtx, taggedQuery, bindVars)
	if err != nil {
		if hasContextError(baseCtx) {
			ar.killRunningQueryByMarker(marker)
		}

		return &contextResultset{empty: true, ctx: baseCtx}, fmt.Errorf(
			"error in running search %w",
			err,
		)
	}
	if !cursor.HasMore() {
		_ = cursor.Close()

		return &contextResultset{empty: true, ctx: baseCtx}, nil
	}

	return &contextResultset{cursor: cursor, ctx: baseCtx}, nil
}

func (ar *arangorepository) getRow(
	ctx context.Context,
	query string,
	bindVars map[string]any,
) (*contextResult, error) {
	baseCtx := normalizeQueryContext(ctx)
	marker := nextQueryMarker()
	taggedQuery := marker + query
	dbh := ar.database.Handler()
	if err := dbh.ValidateQuery(baseCtx, taggedQuery); err != nil {
		return &contextResult{empty: true, ctx: baseCtx}, fmt.Errorf(
			"error in validating the query %w",
			err,
		)
	}
	queryCtx := queryContextWithMaxRuntime(baseCtx)
	cursor, err := dbh.Query(queryCtx, taggedQuery, bindVars)
	if err != nil {
		if hasContextError(baseCtx) {
			ar.killRunningQueryByMarker(marker)
		}

		return &contextResult{empty: true, ctx: baseCtx}, fmt.Errorf(
			"error in query %w",
			err,
		)
	}
	if !cursor.HasMore() {
		_ = cursor.Close()

		return &contextResult{empty: true, ctx: baseCtx}, nil
	}

	return &contextResult{cursor: cursor, ctx: baseCtx}, nil
}

func (r *contextResultset) IsEmpty() bool {
	return r.empty
}

func (r *contextResultset) Scan() bool {
	if r.empty {
		return false
	}
	if r.cursor.HasMore() {
		return true
	}
	_ = r.cursor.Close()

	return false
}

func assignAnnoDocumentMeta(meta driver.DocumentMeta, iface any) {
	setMeta := func(doc *model.AnnoDoc) *model.AnnoDoc {
		doc.DocumentMeta = meta
		return doc
	}
	toAnnoDoc := func(driver.DocumentMeta) O.Option[*model.AnnoDoc] {
		return O.InstanceOf[*model.AnnoDoc](iface)
	}
	_ = F.Pipe3(
		meta,
		O.FromPredicate(hasMetaKey),
		O.Chain(toAnnoDoc),
		O.Map(setMeta),
	)
}

func (r *contextResultset) Read(iface any) error {
	if r.empty {
		return errors.New("cannot read from empty resultset")
	}
	meta, err := r.cursor.ReadDocument(r.ctx, iface)
	if err != nil {
		return fmt.Errorf("error in reading document %w", err)
	}
	assignAnnoDocumentMeta(meta, iface)

	return nil
}

func (r *contextResultset) Close() error {
	if r.empty || r.cursor == nil {
		return nil
	}
	if err := r.cursor.Close(); err != nil {
		return fmt.Errorf("error in closing cursor %w", err)
	}

	return nil
}

func (r *contextResult) IsEmpty() bool {
	return r.empty
}

func (r *contextResult) Read(iface any) error {
	if r.empty || r.cursor == nil {
		return errors.New("cannot read from empty result")
	}
	defer func() { _ = r.cursor.Close() }()
	meta, err := r.cursor.ReadDocument(r.ctx, iface)
	if err != nil {
		return fmt.Errorf("error in reading document %w", err)
	}
	assignAnnoDocumentMeta(meta, iface)

	return nil
}

// scanSearchRows reads every row of a multi-row query result into a slice of T
// and always closes the result set. An unmatched query yields an empty slice
// rather than an error.
//
// fp-go:allow-trycatch-iferr: the ArangoDB driver reports a read fault only
// from inside the scan loop, so this callback is the one place in the package
// that must branch on an error inside IOE.TryCatchError.
func scanSearchRows[T any](
	msg string,
) func(dbRows) IOE.IOEither[error, []T] {
	return func(rows dbRows) IOE.IOEither[error, []T] {
		return IOE.TryCatchError(func() ([]T, error) {
			defer func() { _ = rows.Close() }()

			out := make([]T, 0)
			if rows.IsEmpty() {
				return out, nil
			}
			for rows.Scan() {
				var row T
				if err := rows.Read(&row); err != nil {
					return out, fmt.Errorf("%s %w", msg, err)
				}
				out = append(out, row)
			}

			return out, nil
		})
	}
}

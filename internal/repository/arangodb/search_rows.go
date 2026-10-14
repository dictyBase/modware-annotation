package arangodb

import (
	"fmt"

	IOE "github.com/IBM/fp-go/v2/ioeither"
)

// dbRows is the read side of a multi-row query result. It is an interface so
// the row scanner stays independent of the concrete ArangoDB result set.
type dbRows interface {
	IsEmpty() bool
	Scan() bool
	Read(iface any) error
	Close() error
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

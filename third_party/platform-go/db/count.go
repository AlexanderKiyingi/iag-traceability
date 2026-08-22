package db

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Bounded pagination totals.
//
// Every list endpoint on the platform answers `{items, total, limit, offset}`,
// and the total is computed with COUNT(*) — 229 such sites across roughly
// twenty services at the last audit. COUNT(*) in Postgres always scans: MVCC
// means visibility is per-row, so there is no stored row count to read. An
// index-only scan is the best case and a sequential scan is the common one, and
// either way the cost grows with the table rather than with the page.
//
// On append-only tables — audit logs, event ledgers, telemetry-derived rows —
// that turns a list page into a full scan that gets slower every month, for a
// number a pager cannot use: nobody paginates to row 800,000, and "10,000+"
// serves the UI exactly as well as an exact 2.4 million.
//
// CountBounded stops counting at a cap. The scan is bounded, the number stays
// truthful up to the cap, and IsCapped tells the caller whether to render it as
// an exact figure or a floor.

// DefaultCountCap is the ceiling used by CountBounded when no cap is given.
//
// Chosen to be far past any page a person will actually reach while staying
// cheap on a large table. A UI that wants "about how many" beyond this should
// show "10,000+", not a precise number nobody reads.
const DefaultCountCap = 10000

// Querier is the subset of *pgxpool.Pool / pgx.Tx / pgx.Conn that CountBounded
// needs, so it works inside a transaction as readily as on a pool.
//
// It returns pgx.Row, not a locally-declared interface. Go requires exact
// method signatures for interface satisfaction, so declaring an own Row type
// here — however structurally identical — would mean no real pgx type satisfied
// Querier and every call site failed to compile. The assertions below are what
// catch that, because a hand-written test double satisfies whatever interface
// it is written against and proves nothing about the real one.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var (
	_ Querier = (*pgxpool.Pool)(nil)
	_ Querier = (*pgxpool.Conn)(nil)
	_ Querier = (pgx.Tx)(nil)
)

// CountBounded returns the number of rows matching `fromAndWhere`, stopping at
// cap.
//
// fromAndWhere is everything after SELECT — e.g. `FROM invoices WHERE status =
// $1` — and args bind its placeholders. It is caller-supplied SQL, never user
// input: bind values as parameters, exactly as with any other query.
//
// A cap of zero or less uses DefaultCountCap.
//
// The LIMIT lives in a subquery rather than beside the COUNT, because
// `SELECT COUNT(*) ... LIMIT n` limits the OUTPUT rows — of which there is
// always exactly one — and does nothing at all to the work. Counting the rows
// of an already-limited subquery is what actually bounds the scan.
func CountBounded(ctx context.Context, q Querier, cap int, fromAndWhere string, args ...any) (total int, capped bool, err error) {
	if cap <= 0 {
		cap = DefaultCountCap
	}
	sql := "SELECT COUNT(*) FROM (SELECT 1 " + fromAndWhere +
		" LIMIT " + strconv.Itoa(cap) + ") AS bounded"
	if err := q.QueryRow(ctx, sql, args...).Scan(&total); err != nil {
		return 0, false, fmt.Errorf("bounded count: %w", err)
	}
	return total, total >= cap, nil
}

// IsCapped reports whether a total came back at its cap and is therefore a
// floor ("at least this many") rather than an exact count. Handlers surface
// this so a client can render "10,000+" instead of a wrong exact figure.
func IsCapped(total, cap int) bool {
	if cap <= 0 {
		cap = DefaultCountCap
	}
	return total >= cap
}

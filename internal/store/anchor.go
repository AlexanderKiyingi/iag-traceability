package store

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// AnchorRow is one row of anchor_batches: an L2 notarization of the custody
// blocks in [FromSeq, ToSeq], optionally carrying an L3 public tx reference.
type AnchorRow struct {
	ID          string     `json:"id"`
	FromSeq     int64      `json:"fromSeq"`
	ToSeq       int64      `json:"toSeq"`
	EventCount  int        `json:"eventCount"`
	MerkleRoot  string     `json:"merkleRoot"`
	Algo        string     `json:"algo"`
	KeyID       string     `json:"keyId"`
	Signature   string     `json:"signature"`
	PeriodStart time.Time  `json:"periodStart"`
	PeriodEnd   time.Time  `json:"periodEnd"`
	Provider    string     `json:"provider"`
	TxRef       *string    `json:"txRef,omitempty"`
	AnchoredAt  *time.Time `json:"anchoredAt,omitempty"`
	CreatedAt   time.Time  `json:"createdAt"`
}

// ChainHeadSeq returns the highest custody_chain seq_index, or (0, false) when
// the chain is empty.
func (s *Store) ChainHeadSeq(ctx context.Context) (int64, bool, error) {
	var seq int64
	err := s.pool.QueryRow(ctx, `SELECT seq_index FROM custody_chain ORDER BY seq_index DESC LIMIT 1`).Scan(&seq)
	if err == pgx.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return seq, true, nil
}

// LastAnchoredSeq returns the to_seq of the most recent anchor, or (0, false)
// when nothing has been anchored yet.
func (s *Store) LastAnchoredSeq(ctx context.Context) (int64, bool, error) {
	var seq int64
	err := s.pool.QueryRow(ctx, `SELECT to_seq FROM anchor_batches ORDER BY to_seq DESC LIMIT 1`).Scan(&seq)
	if err == pgx.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return seq, true, nil
}

// ChainLeafRange loads the custody_chain event hashes for seq in (afterSeq, toSeq]
// in order, decoded to 32-byte leaves, along with the occurred_at bounds. Used to
// build a Merkle tree for a new anchor and to reconstruct one for a proof.
func (s *Store) ChainLeafRange(ctx context.Context, afterSeq, toSeq int64) (leaves [][]byte, seqs []int64, start, end time.Time, err error) {
	rows, err := s.pool.Query(ctx, `
		SELECT seq_index, event_hash, occurred_at
		FROM custody_chain
		WHERE seq_index > $1 AND seq_index <= $2
		ORDER BY seq_index ASC`, afterSeq, toSeq)
	if err != nil {
		return nil, nil, time.Time{}, time.Time{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			seq     int64
			hashHex string
			occ     time.Time
		)
		if err := rows.Scan(&seq, &hashHex, &occ); err != nil {
			return nil, nil, time.Time{}, time.Time{}, err
		}
		raw, decErr := hex.DecodeString(hashHex)
		if decErr != nil {
			return nil, nil, time.Time{}, time.Time{}, fmt.Errorf("decode event_hash at seq %d: %w", seq, decErr)
		}
		if len(leaves) == 0 {
			start = occ
		}
		end = occ
		leaves = append(leaves, raw)
		seqs = append(seqs, seq)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, time.Time{}, time.Time{}, err
	}
	return leaves, seqs, start, end, nil
}

// InsertAnchor persists a signed anchor. The UNIQUE(to_seq) constraint makes a
// concurrent double-notarization of the same head fail cleanly rather than
// duplicate.
func (s *Store) InsertAnchor(ctx context.Context, a AnchorRow) (AnchorRow, error) {
	var out AnchorRow
	err := s.pool.QueryRow(ctx, `
		INSERT INTO anchor_batches (
			from_seq, to_seq, event_count, merkle_root, algo, key_id, signature,
			period_start, period_end, provider, tx_ref, anchored_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		RETURNING id, from_seq, to_seq, event_count, merkle_root, algo, key_id, signature,
			period_start, period_end, provider, tx_ref, anchored_at, created_at`,
		a.FromSeq, a.ToSeq, a.EventCount, a.MerkleRoot, a.Algo, a.KeyID, a.Signature,
		a.PeriodStart, a.PeriodEnd, a.Provider, a.TxRef, a.AnchoredAt,
	).Scan(
		&out.ID, &out.FromSeq, &out.ToSeq, &out.EventCount, &out.MerkleRoot, &out.Algo, &out.KeyID, &out.Signature,
		&out.PeriodStart, &out.PeriodEnd, &out.Provider, &out.TxRef, &out.AnchoredAt, &out.CreatedAt,
	)
	return out, err
}

// LatestAnchor returns the most recent anchor, or ErrNotFound when none exist.
func (s *Store) LatestAnchor(ctx context.Context) (AnchorRow, error) {
	return s.scanAnchor(s.pool.QueryRow(ctx, anchorSelect+` ORDER BY to_seq DESC LIMIT 1`))
}

// ListAnchors returns anchors newest-first.
func (s *Store) ListAnchors(ctx context.Context, limit int) ([]AnchorRow, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, anchorSelect+` ORDER BY to_seq DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AnchorRow{}
	for rows.Next() {
		a, err := scanAnchorRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// AnchorCoveringSeq returns the anchor whose range includes seq, or ErrNotFound
// when that event has not been anchored yet.
func (s *Store) AnchorCoveringSeq(ctx context.Context, seq int64) (AnchorRow, error) {
	return s.scanAnchor(s.pool.QueryRow(ctx,
		anchorSelect+` WHERE $1 BETWEEN from_seq AND to_seq ORDER BY to_seq DESC LIMIT 1`, seq))
}

const anchorSelect = `
	SELECT id, from_seq, to_seq, event_count, merkle_root, algo, key_id, signature,
		period_start, period_end, provider, tx_ref, anchored_at, created_at
	FROM anchor_batches`

type rowScanner interface{ Scan(...any) error }

func (s *Store) scanAnchor(row pgx.Row) (AnchorRow, error) {
	a, err := scanAnchorRow(row)
	if err == pgx.ErrNoRows {
		return AnchorRow{}, ErrNotFound
	}
	return a, err
}

func scanAnchorRow(row rowScanner) (AnchorRow, error) {
	var a AnchorRow
	err := row.Scan(
		&a.ID, &a.FromSeq, &a.ToSeq, &a.EventCount, &a.MerkleRoot, &a.Algo, &a.KeyID, &a.Signature,
		&a.PeriodStart, &a.PeriodEnd, &a.Provider, &a.TxRef, &a.AnchoredAt, &a.CreatedAt,
	)
	return a, err
}

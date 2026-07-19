package anchor

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"time"

	"iag-traceability/backend/internal/merkle"
	"iag-traceability/backend/internal/store"
)

// ErrNothingToAnchor means the chain has no new blocks since the last anchor.
var ErrNothingToAnchor = errors.New("anchor: no new custody events since last anchor")

// Service notarizes custody-chain blocks (L2) and optionally publishes the
// signed root to a public ledger (L3).
type Service struct {
	store    *store.Store
	signer   Signer
	provider AnchorProvider
}

func NewService(st *store.Store, signer Signer, provider AnchorProvider) *Service {
	if signer == nil {
		signer = noneSigner{}
	}
	if provider == nil {
		provider = noopProvider{}
	}
	return &Service{store: st, signer: signer, provider: provider}
}

// Signer exposes the active signer so handlers can publish verifier material.
func (s *Service) SignerInfo() (algo, keyID, publicKeyHex string) {
	return s.signer.Algo(), s.signer.KeyID(), s.signer.PublicKeyHex()
}

// CreateAnchor notarizes every custody block appended since the last anchor. It
// is idempotent under concurrency via anchor_batches' UNIQUE(to_seq): a losing
// racer's insert fails and it simply reports ErrNothingToAnchor on retry.
func (s *Service) CreateAnchor(ctx context.Context) (store.AnchorRow, error) {
	head, ok, err := s.store.ChainHeadSeq(ctx)
	if err != nil {
		return store.AnchorRow{}, err
	}
	if !ok {
		return store.AnchorRow{}, ErrNothingToAnchor
	}
	last, _, err := s.store.LastAnchoredSeq(ctx)
	if err != nil {
		return store.AnchorRow{}, err
	}
	if head <= last {
		return store.AnchorRow{}, ErrNothingToAnchor
	}

	leaves, seqs, start, end, err := s.store.ChainLeafRange(ctx, last, head)
	if err != nil {
		return store.AnchorRow{}, err
	}
	if len(leaves) == 0 {
		return store.AnchorRow{}, ErrNothingToAnchor
	}

	root := merkle.Root(leaves)
	rootHex := hex.EncodeToString(root)
	sig, err := s.signer.Sign(root)
	if err != nil {
		return store.AnchorRow{}, fmt.Errorf("sign root: %w", err)
	}

	row := store.AnchorRow{
		FromSeq:     seqs[0],
		ToSeq:       head,
		EventCount:  len(leaves),
		MerkleRoot:  rootHex,
		Algo:        s.signer.Algo(),
		KeyID:       s.signer.KeyID(),
		Signature:   sig,
		PeriodStart: start,
		PeriodEnd:   end,
		Provider:    "none",
	}

	// L3 (optional): post the root on-chain. Any provider error degrades to a
	// valid L2-only anchor — we never fail notarization because a public ledger
	// is unavailable or unconfigured.
	if s.provider.Name() != "none" {
		meta := AnchorMeta{
			Schema:     "iag.custody.anchor.v1",
			EventCount: len(leaves),
			FromSeq:    seqs[0],
			ToSeq:      head,
			PeriodEnd:  end.UTC().Format(time.RFC3339),
			KeyID:      s.signer.KeyID(),
		}
		if txRef, perr := s.provider.PublishRoot(ctx, root, meta); perr == nil && txRef != "" {
			now := time.Now().UTC()
			row.Provider = s.provider.Name()
			row.TxRef = &txRef
			row.AnchoredAt = &now
		} else if perr != nil && !errors.Is(perr, ErrProviderDisabled) && !errors.Is(perr, ErrNotImplemented) {
			log.Printf("[anchor] L3 publish via %s failed (recording L2 only): %v", s.provider.Name(), perr)
		}
	}

	return s.store.InsertAnchor(ctx, row)
}

// Latest returns the most recent anchor.
func (s *Service) Latest(ctx context.Context) (store.AnchorRow, error) {
	return s.store.LatestAnchor(ctx)
}

// List returns anchors newest-first.
func (s *Service) List(ctx context.Context, limit int) ([]store.AnchorRow, error) {
	return s.store.ListAnchors(ctx, limit)
}

// ProofNodeDTO is one sibling on the Merkle path, JSON-friendly.
type ProofNodeDTO struct {
	Hash string `json:"hash"`
	Side string `json:"side"` // "left" | "right" — where the sibling sits
}

// ProofResult is an auditor-verifiable inclusion proof: the event leaf, the
// path to the signed root, and the anchor that attests to it.
type ProofResult struct {
	SeqIndex   int64          `json:"seqIndex"`
	Leaf       string         `json:"leaf"`       // hex event_hash (the leaf pre-image)
	MerkleRoot string         `json:"merkleRoot"` // hex; matches anchor.MerkleRoot
	Path       []ProofNodeDTO `json:"path"`
	Verified   bool           `json:"verified"` // proof reproduces the stored root
	Anchor     store.AnchorRow `json:"anchor"`
}

// Proof builds an inclusion proof for a single custody block against the anchor
// that notarized it. Returns store.ErrNotFound when the block is not yet anchored.
func (s *Service) Proof(ctx context.Context, seq int64) (ProofResult, error) {
	anchor, err := s.store.AnchorCoveringSeq(ctx, seq)
	if err != nil {
		return ProofResult{}, err
	}
	leaves, seqs, _, _, err := s.store.ChainLeafRange(ctx, anchor.FromSeq-1, anchor.ToSeq)
	if err != nil {
		return ProofResult{}, err
	}
	idx := -1
	for i, sq := range seqs {
		if sq == seq {
			idx = i
			break
		}
	}
	if idx < 0 {
		return ProofResult{}, store.ErrNotFound
	}
	path, err := merkle.Proof(leaves, idx)
	if err != nil {
		return ProofResult{}, err
	}
	rootBytes, _ := hex.DecodeString(anchor.MerkleRoot)
	dto := make([]ProofNodeDTO, 0, len(path))
	for _, n := range path {
		side := "left"
		if n.Right {
			side = "right"
		}
		dto = append(dto, ProofNodeDTO{Hash: hex.EncodeToString(n.Hash), Side: side})
	}
	return ProofResult{
		SeqIndex:   seq,
		Leaf:       hex.EncodeToString(leaves[idx]),
		MerkleRoot: anchor.MerkleRoot,
		Path:       dto,
		Verified:   merkle.VerifyProof(leaves[idx], path, rootBytes),
		Anchor:     anchor,
	}, nil
}

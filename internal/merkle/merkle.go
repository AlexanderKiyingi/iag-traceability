// Package merkle builds a binary SHA-256 Merkle tree over custody-chain event
// hashes and produces/validates inclusion proofs. The tree lets a single event
// be verified against a signed root (see internal/anchor) without disclosing or
// walking the rest of the chain.
//
// Conventions (documented so external auditors can reimplement verification):
//   - Leaf hash    = SHA256(0x00 || leaf_bytes)
//   - Internal node = SHA256(0x01 || left || right)
//   - Domain-separation prefixes (0x00 leaf / 0x01 node) prevent second-preimage
//     attacks that equate a leaf with an internal node.
//   - Odd level: the last node is duplicated (hashed with itself) — the common
//     Bitcoin-style convention.
package merkle

import (
	"bytes"
	"crypto/sha256"
	"errors"
)

var (
	leafPrefix = []byte{0x00}
	nodePrefix = []byte{0x01}
)

func hashLeaf(b []byte) []byte {
	h := sha256.New()
	h.Write(leafPrefix)
	h.Write(b)
	return h.Sum(nil)
}

func hashNode(l, r []byte) []byte {
	h := sha256.New()
	h.Write(nodePrefix)
	h.Write(l)
	h.Write(r)
	return h.Sum(nil)
}

// Root returns the Merkle root over leaves. It returns nil for an empty input so
// callers can treat "no events" as "no anchor".
func Root(leaves [][]byte) []byte {
	if len(leaves) == 0 {
		return nil
	}
	level := make([][]byte, len(leaves))
	for i, l := range leaves {
		level[i] = hashLeaf(l)
	}
	for len(level) > 1 {
		level = nextLevel(level)
	}
	return level[0]
}

func nextLevel(level [][]byte) [][]byte {
	next := make([][]byte, 0, (len(level)+1)/2)
	for i := 0; i < len(level); i += 2 {
		if i+1 < len(level) {
			next = append(next, hashNode(level[i], level[i+1]))
		} else {
			// Odd node out: duplicate it (hash with itself).
			next = append(next, hashNode(level[i], level[i]))
		}
	}
	return next
}

// ProofNode is one sibling on the path from a leaf to the root.
type ProofNode struct {
	Hash  []byte
	Right bool // true when this sibling sits on the right of the running hash
}

// Proof returns the inclusion path for leaves[index]. The verifier folds these
// siblings into the leaf hash to reproduce the root.
func Proof(leaves [][]byte, index int) ([]ProofNode, error) {
	if index < 0 || index >= len(leaves) {
		return nil, errors.New("merkle: index out of range")
	}
	level := make([][]byte, len(leaves))
	for i, l := range leaves {
		level[i] = hashLeaf(l)
	}
	var path []ProofNode
	idx := index
	for len(level) > 1 {
		var sibling []byte
		right := false
		if idx%2 == 0 {
			// running hash is the left child; sibling is the right (or self if odd).
			if idx+1 < len(level) {
				sibling = level[idx+1]
			} else {
				sibling = level[idx]
			}
			right = true
		} else {
			sibling = level[idx-1]
			right = false
		}
		path = append(path, ProofNode{Hash: sibling, Right: right})
		idx /= 2
		level = nextLevel(level)
	}
	return path, nil
}

// VerifyProof folds proof into leaf and reports whether it reproduces root.
func VerifyProof(leaf []byte, proof []ProofNode, root []byte) bool {
	running := hashLeaf(leaf)
	for _, n := range proof {
		if n.Right {
			running = hashNode(running, n.Hash)
		} else {
			running = hashNode(n.Hash, running)
		}
	}
	return bytes.Equal(running, root)
}

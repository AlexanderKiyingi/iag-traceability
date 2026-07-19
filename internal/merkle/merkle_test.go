package merkle

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func leaves(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(i+1))
		out[i] = b
	}
	return out
}

func TestRootDeterministic(t *testing.T) {
	a := Root(leaves(5))
	b := Root(leaves(5))
	if !bytes.Equal(a, b) {
		t.Fatal("root not deterministic")
	}
	if Root(nil) != nil {
		t.Fatal("empty root should be nil")
	}
}

func TestRootChangesWithLeaf(t *testing.T) {
	base := leaves(4)
	r1 := Root(base)
	base[2] = []byte("tampered")
	if bytes.Equal(r1, Root(base)) {
		t.Fatal("root must change when a leaf changes")
	}
}

// Every leaf must produce a proof that folds back to the root — for odd and even
// tree sizes, at every index (exercises the duplicate-last-node branch).
func TestProofRoundTrip(t *testing.T) {
	for _, n := range []int{1, 2, 3, 4, 5, 8, 9} {
		ls := leaves(n)
		root := Root(ls)
		for i := 0; i < n; i++ {
			path, err := Proof(ls, i)
			if err != nil {
				t.Fatalf("n=%d i=%d proof: %v", n, i, err)
			}
			if !VerifyProof(ls[i], path, root) {
				t.Fatalf("n=%d i=%d proof did not reproduce root", n, i)
			}
		}
	}
}

func TestProofRejectsWrongLeaf(t *testing.T) {
	ls := leaves(6)
	root := Root(ls)
	path, _ := Proof(ls, 3)
	if VerifyProof([]byte("not the leaf"), path, root) {
		t.Fatal("proof accepted a wrong leaf")
	}
}

func TestProofOutOfRange(t *testing.T) {
	if _, err := Proof(leaves(3), 3); err == nil {
		t.Fatal("expected out-of-range error")
	}
}

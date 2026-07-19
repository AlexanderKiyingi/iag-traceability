// Package anchor implements L2 notarization (sign a Merkle root of custody
// events with a platform key) and orchestrates optional L3 public anchoring.
package anchor

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// Signer signs a Merkle root so a third party can confirm the platform attested
// to it. Keeping this an interface lets us swap the dev HMAC/ed25519 signers for
// a Vault or Cloud-KMS transit signer later without touching the anchor service.
type Signer interface {
	// Algo is the stored algorithm tag (ed25519 | hmac-sha256 | none).
	Algo() string
	// KeyID identifies the key for rotation and is stored on each anchor.
	KeyID() string
	// Sign returns the hex signature over root. A none-signer returns "".
	Sign(root []byte) (string, error)
	// PublicKeyHex is the verifier material (ed25519 public key). Empty for
	// symmetric/none signers.
	PublicKeyHex() string
}

// SignerConfig selects and configures the signing backend.
type SignerConfig struct {
	// Algo: none | hmac-sha256 | ed25519 | vault-transit.
	Algo  string
	KeyID string
	// KeyHex is the raw key material for the in-process signers (HMAC secret or
	// ed25519 seed). Ignored for vault-transit.
	KeyHex string
	// Vault Transit (only for Algo=vault-transit). The private key never leaves
	// Vault; we call it to sign and fetch the public key for verification.
	VaultAddr  string
	VaultToken string
	VaultMount string // default "transit"
	VaultKey   string
}

// NewSigner builds a Signer from config.
//   - "none"          → unsigned notarization (root only; useful for L1-only proofs)
//   - "hmac-sha256"   → symmetric; KeyHex is the shared secret
//   - "ed25519"       → asymmetric, in-process; KeyHex is a 32-byte seed
//   - "vault-transit" → asymmetric, key held in HashiCorp Vault (see vault.go)
//
// vault-transit produces a stored signature byte-for-byte identical to the
// in-process ed25519 signer, so auditor verification is the same regardless of
// where the key lives.
func NewSigner(cfg SignerConfig) (Signer, error) {
	switch strings.ToLower(strings.TrimSpace(cfg.Algo)) {
	case "", "none":
		return noneSigner{}, nil
	case "hmac-sha256", "hmac":
		key, err := hex.DecodeString(strings.TrimSpace(cfg.KeyHex))
		if err != nil || len(key) == 0 {
			return nil, fmt.Errorf("hmac signer: ANCHOR_SIGNING_KEY must be non-empty hex")
		}
		return &hmacSigner{keyID: cfg.KeyID, key: key}, nil
	case "ed25519":
		seed, err := hex.DecodeString(strings.TrimSpace(cfg.KeyHex))
		if err != nil || len(seed) != ed25519.SeedSize {
			return nil, fmt.Errorf("ed25519 signer: ANCHOR_SIGNING_KEY must be %d-byte hex seed", ed25519.SeedSize)
		}
		return &ed25519Signer{keyID: cfg.KeyID, priv: ed25519.NewKeyFromSeed(seed)}, nil
	case "vault-transit", "vault":
		return newVaultSigner(cfg)
	default:
		return nil, fmt.Errorf("unknown ANCHOR_SIGNING_ALGO %q", cfg.Algo)
	}
}

type noneSigner struct{}

func (noneSigner) Algo() string                { return "none" }
func (noneSigner) KeyID() string               { return "" }
func (noneSigner) Sign([]byte) (string, error) { return "", nil }
func (noneSigner) PublicKeyHex() string        { return "" }

type hmacSigner struct {
	keyID string
	key   []byte
}

func (s *hmacSigner) Algo() string  { return "hmac-sha256" }
func (s *hmacSigner) KeyID() string { return s.keyID }
func (s *hmacSigner) Sign(root []byte) (string, error) {
	m := hmac.New(sha256.New, s.key)
	m.Write(root)
	return hex.EncodeToString(m.Sum(nil)), nil
}
func (s *hmacSigner) PublicKeyHex() string { return "" }

type ed25519Signer struct {
	keyID string
	priv  ed25519.PrivateKey
}

func (s *ed25519Signer) Algo() string  { return "ed25519" }
func (s *ed25519Signer) KeyID() string { return s.keyID }
func (s *ed25519Signer) Sign(root []byte) (string, error) {
	return hex.EncodeToString(ed25519.Sign(s.priv, root)), nil
}
func (s *ed25519Signer) PublicKeyHex() string {
	return hex.EncodeToString(s.priv.Public().(ed25519.PublicKey))
}

package anchor

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// vaultSigner signs Merkle roots with an ed25519 key held in HashiCorp Vault's
// Transit engine. The private key never leaves Vault: we POST the root to
// transit/sign and store the returned signature. Because Transit ed25519 signs
// the raw input (no prehash) and we hex-encode the 64-byte signature, the stored
// value is identical to what internal ed25519Signer produces — an auditor
// verifies both the same way with the exported public key.
//
// Implemented over net/http (no Vault SDK) to keep the dependency surface flat,
// matching the rest of the service's thin-client style.
type vaultSigner struct {
	addr      string
	token     string
	mount     string
	key       string
	keyID     string
	publicHex string
	http      *http.Client
}

func newVaultSigner(cfg SignerConfig) (Signer, error) {
	addr := strings.TrimRight(strings.TrimSpace(cfg.VaultAddr), "/")
	mount := strings.Trim(strings.TrimSpace(cfg.VaultMount), "/")
	if mount == "" {
		mount = "transit"
	}
	key := strings.TrimSpace(cfg.VaultKey)
	token := strings.TrimSpace(cfg.VaultToken)
	if addr == "" || key == "" || token == "" {
		return nil, fmt.Errorf("vault signer: VAULT_ADDR, VAULT_TOKEN and VAULT_TRANSIT_KEY are required")
	}
	keyID := cfg.KeyID
	if keyID == "" {
		keyID = "vault:" + mount + "/" + key
	}
	s := &vaultSigner{
		addr:  addr,
		token: token,
		mount: mount,
		key:   key,
		keyID: keyID,
		http:  &http.Client{Timeout: 15 * time.Second},
	}
	// Fetch the public key once at construction so a misconfigured key/mount/token
	// fails fast at boot rather than on the first anchor.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pub, err := s.fetchPublicKeyHex(ctx)
	if err != nil {
		return nil, fmt.Errorf("vault signer: %w", err)
	}
	s.publicHex = pub
	return s, nil
}

func (s *vaultSigner) Algo() string         { return "ed25519" }
func (s *vaultSigner) KeyID() string        { return s.keyID }
func (s *vaultSigner) PublicKeyHex() string { return s.publicHex }

func (s *vaultSigner) Sign(root []byte) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// ed25519 Transit keys sign the raw input message and return a raw 64-byte
	// signature — no prehash and no ECDSA marshaling options apply.
	body := map[string]any{
		"input": base64.StdEncoding.EncodeToString(root),
	}
	var out struct {
		Data struct {
			Signature string `json:"signature"`
		} `json:"data"`
	}
	if err := s.do(ctx, http.MethodPost, "/sign/"+s.key, body, &out); err != nil {
		return "", err
	}
	// Vault returns "vault:v<n>:<base64sig>". Store the raw signature as hex so it
	// matches the in-process ed25519 signer's output format.
	raw := out.Data.Signature
	if i := strings.LastIndex(raw, ":"); i >= 0 {
		raw = raw[i+1:]
	}
	sig, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return "", fmt.Errorf("vault signer: decode signature: %w", err)
	}
	return hex.EncodeToString(sig), nil
}

func (s *vaultSigner) fetchPublicKeyHex(ctx context.Context) (string, error) {
	var out struct {
		Data struct {
			Keys map[string]struct {
				PublicKey string `json:"public_key"`
			} `json:"keys"`
			LatestVersion int `json:"latest_version"`
		} `json:"data"`
	}
	if err := s.do(ctx, http.MethodGet, "/keys/"+s.key, nil, &out); err != nil {
		return "", err
	}
	if len(out.Data.Keys) == 0 {
		return "", fmt.Errorf("no key versions returned for %q", s.key)
	}
	// Prefer the latest version; fall back to any present version.
	ver := fmt.Sprintf("%d", out.Data.LatestVersion)
	entry, ok := out.Data.Keys[ver]
	if !ok {
		for _, e := range out.Data.Keys {
			entry = e
			break
		}
	}
	if entry.PublicKey == "" {
		return "", fmt.Errorf("key %q has no public_key (is it an ed25519 key?)", s.key)
	}
	pub, err := base64.StdEncoding.DecodeString(entry.PublicKey)
	if err != nil {
		return "", fmt.Errorf("decode public_key: %w", err)
	}
	return hex.EncodeToString(pub), nil
}

func (s *vaultSigner) do(ctx context.Context, method, path string, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	url := s.addr + "/v1/" + s.mount + path
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return err
	}
	req.Header.Set("X-Vault-Token", s.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("vault %s %s: %d %s", method, path, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

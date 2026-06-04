package scmclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/alvor-technologies/iag-platform-go/serviceauth"
)

// Client fetches operational snapshots from iag-supply-chain for story composition.
type Client struct {
	baseURL string
	http    *http.Client
	auth    *serviceauth.Client
}

func New(baseURL, tokenURL, clientID, clientSecret, audience string) *Client {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return nil
	}
	c := &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 12 * time.Second},
	}
	if tokenURL != "" && clientID != "" && clientSecret != "" {
		c.auth = serviceauth.NewClient(serviceauth.Options{
			TokenURL: tokenURL, ClientID: clientID, ClientSecret: clientSecret, Audience: audience,
		})
	}
	return c
}

func (c *Client) Enabled() bool { return c != nil && c.baseURL != "" }

type ExportLot struct {
	BusinessID  string   `json:"business_id"`
	BuyerName   string   `json:"buyer_name"`
	Destination string   `json:"destination"`
	CoaNumber   *string  `json:"coa_number"`
	BatchIDs    []string `json:"batch_ids"`
}

func (c *Client) GetExportLot(ctx context.Context, businessID string) (*ExportLot, error) {
	if !c.Enabled() {
		return nil, fmt.Errorf("scm client disabled")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/export-lots/"+businessID, nil)
	if err != nil {
		return nil, err
	}
	if c.auth != nil {
		if err := c.auth.AuthorizeRequest(ctx, req); err != nil {
			return nil, err
		}
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("scm %s: %s", resp.Status, string(b))
	}
	var wrap struct {
		Data ExportLot `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&wrap); err != nil {
		return nil, err
	}
	return &wrap.Data, nil
}

func (c *Client) GetQRPreview(ctx context.Context, lotBusinessID string) (map[string]any, error) {
	if !c.Enabled() {
		return nil, fmt.Errorf("scm client disabled")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v1/export-lots/"+lotBusinessID+"/qr-preview", nil)
	if err != nil {
		return nil, err
	}
	if c.auth != nil {
		if err := c.auth.AuthorizeRequest(ctx, req); err != nil {
			return nil, err
		}
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("scm %s: %s", resp.Status, string(b))
	}
	var wrap struct {
		Data map[string]any `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&wrap); err != nil {
		return nil, err
	}
	return wrap.Data, nil
}

package story

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"iag-traceability/backend/internal/scmclient"
	"iag-traceability/backend/internal/store"
)

type PublicPayload struct {
	Brand     string         `json:"brand"`
	LotID     string         `json:"lot_business_id"`
	Journey   []any          `json:"journey"`
	Standards []string       `json:"standards"`
	PublicURL string         `json:"public_url"`
	Story     map[string]any `json:"story"`
	Status    string         `json:"status"`
}

var scmFetcher *scmclient.Client

// SetSCMClient configures optional SCM snapshot fetcher for story composition.
func SetSCMClient(c *scmclient.Client) {
	scmFetcher = c
}

func randomToken() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func PublishQR(ctx context.Context, st *store.Store, lotBusinessID, baseURL string) (token, publicURL string, err error) {
	token = randomToken()
	publicURL = baseURL + "/" + token
	if err := st.UpsertLotQR(ctx, lotBusinessID, token); err != nil {
		return "", "", err
	}
	// Compose and persist the FULL story (journey, farms, farmers, product), not
	// a 3-field stub. The stored projection is the fallback served on a public
	// scan when a live SCM rebuild fails; writing a stub here would blank the
	// story for every visitor whenever SCM is briefly unavailable.
	composed, cerr := RebuildLotProjection(ctx, st, scmFetcher, lotBusinessID, publicURL)
	if cerr != nil {
		composed = map[string]any{"lot_business_id": lotBusinessID}
	}
	composed["lot_business_id"] = lotBusinessID
	composed["status"] = "published"
	composed["public_url"] = publicURL
	_ = st.UpsertStoryProjection(ctx, lotBusinessID, composed)
	return token, publicURL, nil
}

func ResolvePublicQR(ctx context.Context, st *store.Store, token, baseURL string) (*PublicPayload, error) {
	return resolvePublicQR(ctx, st, token, baseURL, true)
}

// ResolvePublicQRNoCount resolves a public QR without incrementing the scan
// counter. Used by the QR-image endpoint, which is fetched alongside (or instead
// of) the JSON payload and would otherwise double- or over-count scans.
func ResolvePublicQRNoCount(ctx context.Context, st *store.Store, token, baseURL string) (*PublicPayload, error) {
	return resolvePublicQR(ctx, st, token, baseURL, false)
}

func resolvePublicQR(ctx context.Context, st *store.Store, token, baseURL string, countScan bool) (*PublicPayload, error) {
	qr, err := st.GetLotQRByToken(ctx, token)
	if err != nil {
		return nil, err
	}
	if countScan {
		_ = st.IncrementScanCount(ctx, token)
	}

	publicURL := baseURL + "/" + token
	payload := &PublicPayload{
		Brand:     "IAG Africa Coffee Park",
		LotID:     qr.LotBusinessID,
		Standards: []string{"ISO 22005:2007", "GS1 EPCIS 2.0", "EU Reg. 2023/1115", "SCA Protocol"},
		PublicURL: publicURL,
		Status:    "composed",
	}

	if composed, err := RebuildLotProjection(ctx, st, scmFetcher, qr.LotBusinessID, publicURL); err == nil {
		payload.Story = composed
		if j, ok := composed["journey"].([]map[string]any); ok {
			journey := make([]any, len(j))
			for i, step := range j {
				journey[i] = step
			}
			payload.Journey = journey
		}
	} else if cached, err := st.GetStoryProjection(ctx, qr.LotBusinessID); err == nil {
		payload.Story = cached
		payload.Status = "projected"
	} else {
		payload.Story = map[string]any{
			"lot_business_id": qr.LotBusinessID,
			"message":         fmt.Sprintf("Story pending for lot %s", qr.LotBusinessID),
		}
		payload.Status = "pending"
	}
	return payload, nil
}

func BuildChainFromEvents(events []store.TraceEvent) map[string]any {
	nodes := make([]map[string]any, 0, len(events))
	for _, ev := range events {
		nodes = append(nodes, map[string]any{
			"event_type":         ev.EventType,
			"occurred_at":        ev.OccurredAt,
			"entity_type":        ev.EntityType,
			"entity_business_id": ev.EntityBusinessID,
			"source_service":     ev.SourceService,
		})
	}
	return map[string]any{
		"nodes":  nodes,
		"source": "trace_events",
	}
}

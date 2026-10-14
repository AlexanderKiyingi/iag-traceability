package story

import (
	"context"
	"testing"

	"iag-traceability/backend/internal/store"
	"iag-traceability/backend/internal/testdb"
)

func strp(s string) *string { return &s }

func qrRow(t *testing.T, st *store.Store, lot string) (token string, version int) {
	t.Helper()
	if err := st.Pool().QueryRow(context.Background(),
		`SELECT public_token, version FROM lot_qr_codes WHERE lot_business_id = $1`, lot,
	).Scan(&token, &version); err != nil {
		t.Fatalf("qr row: %v", err)
	}
	return token, version
}

// TestStoryOverride_Integration: an override is served by the public read,
// survives the composer rebuilding the projection, clears on "", and never
// touches the QR token or version.
func TestStoryOverride_Integration(t *testing.T) {
	pool := testdb.Pool(t)
	st := store.New(pool)
	ctx := context.Background()
	const lot = "LOT-OVR-1"
	const base = "https://trace.example/q"

	if err := st.UpsertLotQR(ctx, lot, "tok-ovr-1"); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, st, store.AppendEventInput{EventType: "LOT_ASSEMBLED", EntityType: "lot", EntityBusinessID: lot})
	if err := ComposeAndStoreLotStory(ctx, st, lot, base+"/tok-ovr-1"); err != nil {
		t.Fatal(err)
	}
	tok0, ver0 := qrRow(t, st, lot)

	// Unknown lot → not found, nothing written.
	if err := st.SetStoryOverride(ctx, "LOT-NOPE", store.StoryOverridePatch{StoryTitle: strp("x")}); err != store.ErrNotFound {
		t.Fatalf("unknown lot: %v, want ErrNotFound", err)
	}

	if err := st.SetStoryOverride(ctx, lot, store.StoryOverridePatch{
		StoryTitle: strp("  Grown on Mt Elgon "), StoryBody: strp("Hand picked."), UpdatedBy: "Ada",
	}); err != nil {
		t.Fatal(err)
	}
	if tok, ver := qrRow(t, st, lot); tok != tok0 || ver != ver0 {
		t.Fatalf("override changed the QR: %s/%d -> %s/%d", tok0, ver0, tok, ver)
	}

	pub, err := ResolvePublicQR(ctx, st, tok0, base)
	if err != nil {
		t.Fatal(err)
	}
	if pub.Story["story_title"] != "Grown on Mt Elgon" || pub.Story["story_body"] != "Hand picked." {
		t.Fatalf("public story missing override: %v", pub.Story)
	}
	if pub.Story["journey"] == nil {
		t.Fatal("the composed journey must stay alongside the override")
	}

	// The stored projection (the public fallback) mirrors the override.
	proj, _ := st.GetStoryProjection(ctx, lot)
	if proj["story_title"] != "Grown on Mt Elgon" {
		t.Fatalf("projection not mirrored: %v", proj["story_title"])
	}

	// A composer rebuild rewrites the projection wholesale; the override stays.
	if ok, err := RebuildStoredLot(ctx, st, lot, ""); err != nil || !ok {
		t.Fatalf("rebuild: ok=%v err=%v", ok, err)
	}
	proj, _ = st.GetStoryProjection(ctx, lot)
	if proj["story_title"] != "Grown on Mt Elgon" || proj["story_body"] != "Hand picked." {
		t.Fatalf("override lost on rebuild: %v", proj)
	}
	if ov, _ := st.GetStoryOverride(ctx, lot); ov == nil || ov.UpdatedBy != "Ada" {
		t.Fatalf("override row after rebuild: %+v", ov)
	}

	// Partial patch: body only; title untouched.
	if err := st.SetStoryOverride(ctx, lot, store.StoryOverridePatch{StoryBody: strp("Sun dried."), UpdatedBy: "Ben"}); err != nil {
		t.Fatal(err)
	}
	ov, _ := st.GetStoryOverride(ctx, lot)
	if ov == nil || ov.StoryTitle == nil || *ov.StoryTitle != "Grown on Mt Elgon" || *ov.StoryBody != "Sun dried." {
		t.Fatalf("partial patch: %+v", ov)
	}

	// "" clears; clearing both removes the override entirely.
	if err := st.SetStoryOverride(ctx, lot, store.StoryOverridePatch{StoryTitle: strp(""), UpdatedBy: "Ben"}); err != nil {
		t.Fatal(err)
	}
	pub, _ = ResolvePublicQR(ctx, st, tok0, base)
	if _, has := pub.Story["story_title"]; has || pub.Story["story_body"] != "Sun dried." {
		t.Fatalf("title should be cleared, body kept: %v", pub.Story)
	}
	if err := st.SetStoryOverride(ctx, lot, store.StoryOverridePatch{StoryBody: strp("   "), UpdatedBy: "Ben"}); err != nil {
		t.Fatal(err)
	}
	if ov, _ := st.GetStoryOverride(ctx, lot); ov != nil {
		t.Fatalf("both cleared should remove the override: %+v", ov)
	}
	proj, _ = st.GetStoryProjection(ctx, lot)
	if _, has := proj["story_body"]; has {
		t.Fatalf("cleared override lingers in projection: %v", proj)
	}

	// Every edit is audited, newest first.
	audit, err := st.ListStoryOverrideAudit(ctx, lot, 10)
	if err != nil || len(audit) != 4 {
		t.Fatalf("audit rows = %d (%v), want 4", len(audit), err)
	}
	if audit[3].UpdatedBy != "Ada" || audit[3].PreviousTitle != nil || audit[0].PreviousBody == nil {
		t.Fatalf("audit content: first=%+v last=%+v", audit[3], audit[0])
	}

	if tok, ver := qrRow(t, st, lot); tok != tok0 || ver != ver0 {
		t.Fatalf("QR changed during edits: %s/%d -> %s/%d", tok0, ver0, tok, ver)
	}
}

// TestCustodyTransfer_ChainIntegration: transfer events take the same append
// path, including the custody chain when it is on, and compose into the
// journey without leaking from/to.
func TestCustodyTransfer_ChainIntegration(t *testing.T) {
	pool := testdb.Pool(t)
	st := store.New(pool).SetChainEnabled(true)
	ctx := context.Background()

	payload := map[string]any{"from": "Wet mill A", "to": "Dry mill B", "quantity_kg": 300.0}
	mustAppend(t, st, store.AppendEventInput{EventType: CustodyTransferred, EntityType: "batch", EntityBusinessID: "BAT-T1", Payload: payload})
	mustAppend(t, st, store.AppendEventInput{EventType: CustodyReceived, EntityType: "batch", EntityBusinessID: "BAT-T1", Payload: payload})
	mustAppend(t, st, store.AppendEventInput{EventType: CustodyInTransit, EntityType: "lot", EntityBusinessID: "LOT-T1", Payload: payload})

	var chained int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*)::int FROM custody_chain WHERE batch_business_id = 'BAT-T1'`).Scan(&chained); err != nil {
		t.Fatal(err)
	}
	if chained != 2 {
		t.Fatalf("custody_chain rows = %d, want 2 (batch transfers chain)", chained)
	}
	if res, err := st.VerifyChain(ctx); err != nil || !res.IsValid {
		t.Fatalf("chain invalid after transfers: %+v %v", res, err)
	}

	composed, err := RebuildLotProjection(ctx, st, nil, "LOT-T1", "")
	if err != nil {
		t.Fatal(err)
	}
	journey, _ := composed["journey"].([]map[string]any)
	if len(journey) != 1 || journey[0]["stage"] != "Custody" {
		t.Fatalf("journey = %v", journey)
	}
	for k := range journey[0] {
		if k == "from" || k == "to" || k == "payload" {
			t.Fatalf("journey leaks %s", k)
		}
	}
}

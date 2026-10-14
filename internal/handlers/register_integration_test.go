package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alvor-technologies/iag-platform-go/authclient"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"iag-traceability/backend/internal/config"
	"iag-traceability/backend/internal/ctxkeys"
	appmw "iag-traceability/backend/internal/middleware"
	"iag-traceability/backend/internal/store"
	"iag-traceability/backend/internal/testdb"
)

const testBase = "https://trace.example/q"

// testEngine mounts the routes under test with the router's permissions and
// a principal taken from X-Test-Perms (comma list), standing in for the
// platform auth middleware.
func testEngine(api *API) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(func(c *gin.Context) {
		perms := strings.Split(c.GetHeader("X-Test-Perms"), ",")
		uid := uuid.MustParse("11111111-2222-3333-4444-555555555555")
		c.Set(ctxkeys.UserID, uid)
		c.Set(ctxkeys.Claims, &authclient.Claims{Name: "Test Editor", Permissions: perms})
		c.Next()
	})
	r.GET("/public/q/:token", api.PublicQR)
	v1 := r.Group("/api/v1")
	v1.GET("/events", appmw.RequirePermission("traceability.view_events"), api.ListEvents)
	v1.POST("/events", appmw.RequirePermission("traceability.add_trace_event"), api.RecordEvent)
	v1.POST("/lots/:businessId/revoke", appmw.RequirePermission("traceability.publish_qr"), api.RevokeLotQR)
	v1.GET("/qr-codes", appmw.RequirePermission("traceability.view_chain"), api.ListQRCodes)
	v1.GET("/qr-codes/:lotBusinessId", appmw.RequirePermission("traceability.view_chain"), api.GetQRCode)
	v1.PATCH("/qr-codes/:lotBusinessId", appmw.RequirePermission("traceability.publish_qr"), api.PatchQRStory)
	return r
}

func do(t *testing.T, r http.Handler, method, path, perms string, body any) (int, map[string]any) {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = bytes.NewReader(raw)
	} else {
		rd = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rd)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Test-Perms", perms)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	out := map[string]any{}
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out
}

func TestRouterMountsRegisterRoutes(t *testing.T) {
	r := NewRouter(RouterDeps{API: &API{Cfg: &config.Config{}}})
	want := map[string]bool{
		"GET /api/v1/qr-codes":                  false,
		"GET /api/v1/qr-codes/:lotBusinessId":   false,
		"PATCH /api/v1/qr-codes/:lotBusinessId": false,
		"GET /api/v1/events":                    false,
		"POST /api/v1/events":                   false,
		"POST /api/v1/lots/:businessId/revoke":  false,
	}
	for _, rt := range r.Routes() {
		k := rt.Method + " " + rt.Path
		if _, ok := want[k]; ok {
			want[k] = true
		}
	}
	for k, seen := range want {
		if !seen {
			t.Errorf("route %s not mounted", k)
		}
	}
}

func TestEventsTransfersAndFilters_Integration(t *testing.T) {
	pool := testdb.Pool(t)
	st := store.New(pool)
	r := testEngine(&API{Cfg: &config.Config{PublicTraceBaseURL: testBase}, Store: st})
	const add = "traceability.add_trace_event"
	const view = "traceability.view_events"

	transfer := func(payload map[string]any, entityType string) map[string]any {
		return map[string]any{
			"event_type": "CUSTODY_TRANSFERRED", "entity_type": entityType,
			"entity_business_id": "LOT-EV-1", "payload": payload,
			"occurred_at": "2026-10-02T09:00:00Z",
		}
	}
	good := map[string]any{"from": "Wet mill A", "to": "Warehouse 2", "quantity_kg": 120.5}

	// Permission unchanged.
	if code, _ := do(t, r, "POST", "/api/v1/events", view, transfer(good, "lot")); code != http.StatusForbidden {
		t.Fatalf("without add_trace_event: %d", code)
	}
	for name, body := range map[string]map[string]any{
		"missing from":   transfer(map[string]any{"to": "b", "quantity_kg": 1}, "lot"),
		"empty to":       transfer(map[string]any{"from": "a", "to": "", "quantity_kg": 1}, "lot"),
		"negative kg":    transfer(map[string]any{"from": "a", "to": "b", "quantity_kg": -1}, "lot"),
		"string kg":      transfer(map[string]any{"from": "a", "to": "b", "quantity_kg": "1"}, "lot"),
		"party transfer": transfer(good, "party"),
	} {
		if code, out := do(t, r, "POST", "/api/v1/events", add, body); code != http.StatusBadRequest {
			t.Fatalf("%s: %d %v", name, code, out)
		}
	}
	code, created := do(t, r, "POST", "/api/v1/events", add, transfer(good, "lot"))
	if code != http.StatusCreated {
		t.Fatalf("valid transfer: %d %v", code, created)
	}
	recv := map[string]any{"event_type": "CUSTODY_RECEIVED", "entity_type": "batch", "entity_business_id": "BAT-EV-1",
		"payload": good, "occurred_at": "2026-10-05T09:00:00Z"}
	if code, out := do(t, r, "POST", "/api/v1/events", add, recv); code != http.StatusCreated {
		t.Fatalf("valid receipt: %d %v", code, out)
	}
	if code, out := do(t, r, "POST", "/api/v1/events", add, map[string]any{
		"event_type": "LAB_RESULT_RECORDED", "entity_type": "batch", "entity_business_id": "BAT-EV-1",
		"occurred_at": "2026-10-03T09:00:00Z"}); code != http.StatusCreated {
		t.Fatalf("lab result: %d %v", code, out)
	}

	list := func(q string) []any {
		t.Helper()
		code, out := do(t, r, "GET", "/api/v1/events"+q, view, nil)
		if code != http.StatusOK {
			t.Fatalf("GET events%s: %d %v", q, code, out)
		}
		evs, _ := out["events"].([]any)
		if _, ok := out["has_more"]; !ok {
			t.Fatal("has_more missing")
		}
		return evs
	}
	if got := list(""); len(got) != 3 {
		t.Fatalf("global list = %d, want 3", len(got))
	}
	// Newest first; payload carried through.
	all := list("")
	first := all[0].(map[string]any)
	if first["event_type"] != "CUSTODY_RECEIVED" {
		t.Fatalf("global order: first = %v", first["event_type"])
	}
	p := first["payload"].(map[string]any)
	if p["from"] != "Wet mill A" || p["to"] != "Warehouse 2" || p["quantity_kg"] != 120.5 {
		t.Fatalf("payload not returned: %v", p)
	}
	if got := list("?event_type=CUSTODY_TRANSFERRED,CUSTODY_RECEIVED,CUSTODY_IN_TRANSIT"); len(got) != 2 {
		t.Fatalf("event_type filter = %d, want 2", len(got))
	}
	if got := list("?entity_type=batch"); len(got) != 2 {
		t.Fatalf("entity_type filter = %d, want 2", len(got))
	}
	if got := list("?from=2026-10-03&to=2026-10-05"); len(got) != 2 {
		t.Fatalf("date range (to inclusive of day) = %d, want 2", len(got))
	}
	if got := list("?to=2026-10-05T09:00:00Z"); len(got) != 2 {
		t.Fatalf("rfc3339 to is exclusive = %d, want 2", len(got))
	}
	if got := list("?entity_type=lot&entity_business_id=LOT-EV-1&event_type=CUSTODY_TRANSFERRED"); len(got) != 1 {
		t.Fatalf("combined filter = %d, want 1", len(got))
	}
	if code, _ := do(t, r, "GET", "/api/v1/events?entity_business_id=X", view, nil); code != http.StatusBadRequest {
		t.Fatalf("id without type: %d", code)
	}
	if code, _ := do(t, r, "GET", "/api/v1/events?from=nope", view, nil); code != http.StatusBadRequest {
		t.Fatalf("bad from: %d", code)
	}
}

func seedQR(t *testing.T, st *store.Store, lot, token string, payload map[string]any) {
	t.Helper()
	ctx := context.Background()
	if err := st.UpsertLotQR(ctx, lot, token); err != nil {
		t.Fatal(err)
	}
	if payload != nil {
		if err := st.UpsertStoryProjection(ctx, lot, payload); err != nil {
			t.Fatal(err)
		}
	}
}

func TestQRRegister_Integration(t *testing.T) {
	pool := testdb.Pool(t)
	st := store.New(pool)
	r := testEngine(&API{Cfg: &config.Config{PublicTraceBaseURL: testBase}, Store: st})
	const view = "traceability.view_chain"
	const publish = "traceability.publish_qr"
	ctx := context.Background()

	seedQR(t, st, "LOT-R1", "tok-r1", map[string]any{
		"farmers": []any{map[string]any{"name": "Akello Grace"}, map[string]any{"full_name": "Okello John"}, map[string]any{"business_id": "P-9"}},
	})
	seedQR(t, st, "LOT-R2", "tok-r2", map[string]any{"title": "SCM title"})
	seedQR(t, st, "LOT-R3", "tok-r3", nil)
	if err := st.RevokeLotQR(ctx, "LOT-R3"); err != nil {
		t.Fatal(err)
	}

	list := func(q string) []map[string]any {
		t.Helper()
		code, out := do(t, r, "GET", "/api/v1/qr-codes"+q, view, nil)
		if code != http.StatusOK {
			t.Fatalf("GET qr-codes%s: %d %v", q, code, out)
		}
		var rows []map[string]any
		for _, v := range out["qr_codes"].([]any) {
			rows = append(rows, v.(map[string]any))
		}
		return rows
	}
	if code, _ := do(t, r, "GET", "/api/v1/qr-codes", "traceability.view_events", nil); code != http.StatusForbidden {
		t.Fatalf("register needs view_chain: %d", code)
	}
	rows := list("")
	if len(rows) != 3 {
		t.Fatalf("register = %d rows, want 3", len(rows))
	}
	byLot := map[string]map[string]any{}
	for _, row := range rows {
		byLot[row["lot_business_id"].(string)] = row
	}
	r1 := byLot["LOT-R1"]
	if r1["status"] != "Live" || r1["token"] != "tok-r1" || r1["public_url"] != testBase+"/tok-r1" ||
		r1["version"] != 1.0 || r1["published_at"] == nil || r1["revoked_at"] != nil || r1["story_override"] != nil {
		t.Fatalf("LOT-R1 row: %v", r1)
	}
	if names := r1["farmer_names"].([]any); len(names) != 2 || names[0] != "Akello Grace" || names[1] != "Okello John" {
		t.Fatalf("farmer_names: %v", names)
	}
	if byLot["LOT-R2"]["story_title"] != "SCM title" {
		t.Fatalf("composed title: %v", byLot["LOT-R2"]["story_title"])
	}
	if byLot["LOT-R3"]["status"] != "Revoked" || byLot["LOT-R3"]["revoked_at"] == nil {
		t.Fatalf("revoked row: %v", byLot["LOT-R3"])
	}
	if got := list("?status=live"); len(got) != 2 {
		t.Fatalf("status=live = %d", len(got))
	}
	if got := list("?status=REVOKED"); len(got) != 1 || got[0]["lot_business_id"] != "LOT-R3" {
		t.Fatalf("status=revoked = %v", got)
	}
	if code, _ := do(t, r, "GET", "/api/v1/qr-codes?status=draft", view, nil); code != http.StatusBadRequest {
		t.Fatalf("bad status: %d", code)
	}
	if got := list("?q=akello"); len(got) != 1 || got[0]["lot_business_id"] != "LOT-R1" {
		t.Fatalf("q by farmer = %v", got)
	}
	if got := list("?q=lot-r"); len(got) != 3 {
		t.Fatalf("q by lot id = %d", len(got))
	}
	if got := list("?q=100%25"); len(got) != 0 {
		t.Fatalf("q escapes LIKE wildcards = %d", len(got))
	}
	if code, _ := do(t, r, "GET", "/api/v1/qr-codes/LOT-NONE", view, nil); code != http.StatusNotFound {
		t.Fatalf("missing lot: %d", code)
	}

	// PATCH: permission, validation, effect.
	if code, _ := do(t, r, "PATCH", "/api/v1/qr-codes/LOT-R1", view, map[string]any{"story_title": "x"}); code != http.StatusForbidden {
		t.Fatalf("patch needs publish_qr: %d", code)
	}
	if code, _ := do(t, r, "PATCH", "/api/v1/qr-codes/LOT-R1", publish, map[string]any{}); code != http.StatusBadRequest {
		t.Fatalf("empty patch: %d", code)
	}
	if code, _ := do(t, r, "PATCH", "/api/v1/qr-codes/LOT-NONE", publish, map[string]any{"story_title": "x"}); code != http.StatusNotFound {
		t.Fatalf("patch unknown lot: %d", code)
	}
	before, _ := time.Parse(time.RFC3339Nano, r1["updated_at"].(string))
	code, row := do(t, r, "PATCH", "/api/v1/qr-codes/LOT-R1", publish,
		map[string]any{"story_title": "Mt Elgon washed", "story_body": "Picked by hand."})
	if code != http.StatusOK {
		t.Fatalf("patch: %d %v", code, row)
	}
	if row["token"] != "tok-r1" || row["version"] != 1.0 || row["story_title"] != "Mt Elgon washed" {
		t.Fatalf("patch changed token/version or title missing: %v", row)
	}
	ov := row["story_override"].(map[string]any)
	if ov["story_body"] != "Picked by hand." || ov["updated_by"] != "Test Editor" || ov["updated_by_user_id"] == nil {
		t.Fatalf("story_override: %v", ov)
	}
	// Compared against the database's own clock, not the test host's.
	if ts, _ := time.Parse(time.RFC3339Nano, row["updated_at"].(string)); !ts.After(before) || row["updated_at"] != ov["updated_at"] {
		t.Fatalf("updated_at not bumped to the edit: before=%v row=%v override=%v", before, row["updated_at"], ov["updated_at"])
	}

	// The public read serves the override on the same, unchanged token.
	code, pub := do(t, r, "GET", "/public/q/tok-r1", "", nil)
	if code != http.StatusOK {
		t.Fatalf("public read: %d", code)
	}
	storyMap := pub["story"].(map[string]any)
	if storyMap["story_title"] != "Mt Elgon washed" || storyMap["story_body"] != "Picked by hand." {
		t.Fatalf("public story: %v", storyMap)
	}

	// "" clears the title only.
	_, row = do(t, r, "PATCH", "/api/v1/qr-codes/LOT-R1", publish, map[string]any{"story_title": ""})
	if row["story_title"] != nil || row["story_override"].(map[string]any)["story_body"] != "Picked by hand." {
		t.Fatalf("clear title: %v", row)
	}
	_, pub = do(t, r, "GET", "/public/q/tok-r1", "", nil)
	if _, has := pub["story"].(map[string]any)["story_title"]; has {
		t.Fatalf("cleared title still public: %v", pub["story"])
	}

	// Revoke returns the updated row; the token stops resolving.
	code, rev := do(t, r, "POST", "/api/v1/lots/LOT-R1/revoke", publish, nil)
	if code != http.StatusOK || rev["revoked"] != true {
		t.Fatalf("revoke: %d %v", code, rev)
	}
	if q := rev["qr_code"].(map[string]any); q["status"] != "Revoked" || q["token"] != "tok-r1" {
		t.Fatalf("revoke row: %v", q)
	}
	if code, _ := do(t, r, "GET", "/public/q/tok-r1", "", nil); code != http.StatusNotFound {
		t.Fatalf("revoked token still public: %d", code)
	}
	if code, _ := do(t, r, "POST", "/api/v1/lots/LOT-R1/revoke", publish, nil); code != http.StatusNotFound {
		t.Fatalf("double revoke: %d", code)
	}

	var version int
	var token string
	_ = pool.QueryRow(ctx, `SELECT public_token, version FROM lot_qr_codes WHERE lot_business_id='LOT-R1'`).Scan(&token, &version)
	if token != "tok-r1" || version != 1 {
		t.Fatalf("QR rotated: %s/%d", token, version)
	}
}

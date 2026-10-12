package story

import "testing"

func TestStripCommercialRemovesBuyerEverywhere(t *testing.T) {
	preview := map[string]any{
		"lot":      map[string]any{"business_id": "LOT-1", "buyer_name": "Acme Roasters", "destination": "DE"},
		"contract": map[string]any{"contract_price": 4.1, "buyer": map[string]any{"name": "Acme"}},
		"farmers":  []any{map[string]any{"name": "A", "customer": "x"}},
		"buyer_id": "B-1",
	}
	sanitizePreviewEntities(preview)
	lot := preview["lot"].(map[string]any)
	if _, ok := lot["buyer_name"]; ok || lot["destination"] != "DE" {
		t.Fatalf("lot: %+v", lot)
	}
	if _, ok := preview["buyer_id"]; ok {
		t.Fatal("top-level buyer id")
	}
	contract := preview["contract"].(map[string]any)
	if _, ok := contract["contract_price"]; ok {
		t.Fatalf("price: %+v", contract)
	}
	if _, ok := contract["buyer"]; ok {
		t.Fatalf("nested buyer: %+v", contract)
	}
}

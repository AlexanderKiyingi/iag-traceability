package story

import "testing"

func TestValidateEventType_KnownTypes(t *testing.T) {
	for _, typ := range []string{"CHERRY_RECEIVED", "COA_ISSUED", "PARTY_UPDATED", "FARM_UPDATED", "LOT_ASSEMBLED"} {
		if err := ValidateEventType(typ); err != nil {
			t.Fatalf("expected %s to be allowed: %v", typ, err)
		}
	}
}

func TestValidateEventType_UnknownRejected(t *testing.T) {
	if err := ValidateEventType("NOT_A_REAL_EVENT"); err == nil {
		t.Fatal("expected unknown event type to fail")
	}
}

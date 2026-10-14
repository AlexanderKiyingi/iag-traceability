package story

import (
	"fmt"
	"math"
	"strings"
)

var allowedEventTypes = map[string]bool{
	"FARMER_REGISTERED":      true,
	"CHERRY_RECEIVED":        true,
	"INTAKE_RECEIVED":        true,
	"STAGE_CHANGED":          true,
	"MANUAL_NOTE":            true,
	"LOT_QR_PUBLISHED":       true,
	"CORRECTION":             true,
	"PARTY_CREATED":          true,
	"PARTY_UPDATED":          true,
	"FARM_REGISTERED":        true,
	"FARM_UPDATED":           true,
	"LOT_ASSEMBLED":          true,
	"WET_MILL_STARTED":       true,
	"DRYING_STARTED":         true,
	"SAMPLE_SUBMITTED":       true,
	"WET_MILL_COMPLETE":      true,
	"DRYING_COMPLETE":        true,
	"DRY_MILL_COMPLETE":      true,
	"LAB_RESULT_RECORDED":    true,
	"COA_ISSUED":             true,
	"qc.coa.issued":          true,
	"qc.lab.result_recorded": true,
	CustodyTransferred:       true,
	CustodyInTransit:         true,
	CustodyReceived:          true,
}

// Custody transfer event types. A transfer moves a lot or batch from one
// holder to another; from, to and quantity_kg travel in the payload.
const (
	CustodyTransferred = "CUSTODY_TRANSFERRED"
	CustodyInTransit   = "CUSTODY_IN_TRANSIT"
	CustodyReceived    = "CUSTODY_RECEIVED"
)

// IsCustodyTransfer reports whether eventType is one of the custody transfer
// types.
func IsCustodyTransfer(eventType string) bool {
	switch eventType {
	case CustodyTransferred, CustodyInTransit, CustodyReceived:
		return true
	}
	return false
}

func ValidateEventType(eventType string) error {
	if allowedEventTypes[eventType] {
		return nil
	}
	return fmt.Errorf("unknown event_type: %s", eventType)
}

// ValidateEventPayload applies the per-type rules that go beyond the type
// allowlist. Only the custody transfer types have any today:
//
//   - entity_type must be lot or batch (custody moves goods, not parties);
//   - payload.from and payload.to must be non-empty strings;
//   - payload.quantity_kg must be a number >= 0.
func ValidateEventPayload(eventType, entityType string, payload map[string]any) error {
	if !IsCustodyTransfer(eventType) {
		return nil
	}
	if entityType != "lot" && entityType != "batch" {
		return fmt.Errorf("%s requires entity_type lot or batch", eventType)
	}
	for _, key := range []string{"from", "to"} {
		s, ok := payload[key].(string)
		if !ok || strings.TrimSpace(s) == "" {
			return fmt.Errorf("%s requires payload.%s (non-empty string)", eventType, key)
		}
	}
	raw, present := payload["quantity_kg"]
	if !present {
		return fmt.Errorf("%s requires payload.quantity_kg (number >= 0)", eventType)
	}
	var q float64
	switch n := raw.(type) {
	case float64:
		q = n
	case int:
		q = float64(n)
	case int64:
		q = float64(n)
	default:
		return fmt.Errorf("%s requires payload.quantity_kg to be a number", eventType)
	}
	if math.IsNaN(q) || math.IsInf(q, 0) || q < 0 {
		return fmt.Errorf("%s requires payload.quantity_kg >= 0", eventType)
	}
	return nil
}

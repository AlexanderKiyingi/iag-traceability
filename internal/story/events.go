package story

import (
	"fmt"
)

var allowedEventTypes = map[string]bool{
	"FARMER_REGISTERED":    true,
	"CHERRY_RECEIVED":      true,
	"INTAKE_RECEIVED":      true,
	"STAGE_CHANGED":        true,
	"MANUAL_NOTE":          true,
	"LOT_QR_PUBLISHED":     true,
	"CORRECTION":           true,
	"PARTY_CREATED":        true,
	"FARM_REGISTERED":      true,
	"WET_MILL_COMPLETE":    true,
	"DRYING_COMPLETE":      true,
	"DRY_MILL_COMPLETE":    true,
	"LAB_RESULT_RECORDED":  true,
	"COA_ISSUED":           true,
	"qc.coa.issued":        true,
	"qc.lab.result_recorded": true,
}

func ValidateEventType(eventType string) error {
	if allowedEventTypes[eventType] {
		return nil
	}
	return fmt.Errorf("unknown event_type: %s", eventType)
}

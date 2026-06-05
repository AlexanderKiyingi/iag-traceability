package story

import "errors"

var ErrCoARequired = errors.New("COA_REQUIRED")

// ErrComplianceGateUnavailable is returned when SCM publish-gate is configured but unreachable.
var ErrComplianceGateUnavailable = errors.New("COMPLIANCE_GATE_UNAVAILABLE")

package story

import (
	"errors"
	"testing"
)

func TestIsSCMTransportError(t *testing.T) {
	if !isSCMTransportError(errors.New("scm 503: unavailable")) {
		t.Fatal("expected 503 to be transport error")
	}
	if isSCMTransportError(errors.New("EUDR_GPS_REQUIRED")) {
		t.Fatal("compliance failure should not be transport error")
	}
}

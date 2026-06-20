package story

import (
	"context"
	"errors"
	"fmt"
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

func TestIsSCMTransportError_DeadlineIsTransport(t *testing.T) {
	// A slow SCM surfaces as context.DeadlineExceeded (possibly wrapped). It must
	// be treated as gate-unavailable (retry), not as a compliance failure (422).
	if !isSCMTransportError(context.DeadlineExceeded) {
		t.Fatal("context.DeadlineExceeded should be a transport error")
	}
	if !isSCMTransportError(fmt.Errorf("get qr preview: %w", context.DeadlineExceeded)) {
		t.Fatal("wrapped deadline should be a transport error")
	}
	if !isSCMTransportError(errors.New("Post \"http://scm/validate\": context deadline exceeded")) {
		t.Fatal("deadline string should be a transport error")
	}
	if !isSCMTransportError(context.Canceled) {
		t.Fatal("context.Canceled should be a transport error")
	}
}

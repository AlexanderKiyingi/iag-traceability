package story

// Options configures story composition behaviour.
type Options struct {
	// PlaceholderJourney inserts generic journey steps when no custody events exist.
	// Should be false in production (Phase 6).
	PlaceholderJourney bool
	// PreciseGeo, when false (the default), coarsens farm/farmer GPS coordinates
	// in the public payload to ~1km so the unauthenticated QR story does not leak
	// smallholder farmers' exact locations.
	PreciseGeo bool
}

var opts = Options{PlaceholderJourney: true}

func SetOptions(o Options) {
	opts = o
}

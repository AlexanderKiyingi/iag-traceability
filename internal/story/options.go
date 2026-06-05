package story

// Options configures story composition behaviour.
type Options struct {
	// PlaceholderJourney inserts generic journey steps when no custody events exist.
	// Should be false in production (Phase 6).
	PlaceholderJourney bool
}

var opts = Options{PlaceholderJourney: true}

func SetOptions(o Options) {
	opts = o
}

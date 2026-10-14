package scmsync

import (
	"context"
	"errors"
	"log"
	"time"
)

// tickTimeout bounds one tick so a hung query cannot wedge the loop.
const tickTimeout = 5 * time.Minute

// Loop ticks every interval until ctx is cancelled. A failed or panicking tick
// is logged and retried on the next one; it never stops the loop or the
// server.
func (s *Syncer) Loop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	for {
		s.safeTick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

func (s *Syncer) safeTick(ctx context.Context) {
	defer func() {
		if p := recover(); p != nil {
			log.Printf("[scm-sync] tick panicked (will retry next tick): %v", p)
		}
	}()
	tctx, cancel := context.WithTimeout(ctx, tickTimeout)
	defer cancel()
	res, err := s.Tick(tctx)
	switch {
	case errors.Is(err, ErrLocked):
		// Another replica is syncing; nothing to do.
	case err != nil:
		log.Printf("[scm-sync] tick failed (will retry next tick): %v (%s)", err, res)
	default:
		log.Printf("[scm-sync] tick ok: %s", res)
	}
}

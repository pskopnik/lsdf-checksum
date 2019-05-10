package ratedreader

import (
	"context"
	"io"
	"time"

	"golang.org/x/time/rate"
)

var _ io.Reader = &MultiReader{}

type MultiReader struct {
	r        io.Reader
	limiters []rate.Limiter
	minBurst int
	ctx      context.Context
}

func NewMultiReader(rd io.Reader, limiters []rate.Limiter) *MultiReader {
	if len(limiters) == 0 {
		panic("NewMultiReader: must be initialised with at least one limiter")
	}

	minBurst := limiters[len(limiters)-1].Burst()
	for i := range limiters {
		if limiters[i].Burst() < minBurst {
			minBurst = limiters[i].Burst()
		}
	}

	return &MultiReader{
		r:        rd,
		limiters: limiters,
		minBurst: minBurst,
		ctx:      context.Background(),
	}
}

func (m *MultiReader) Read(p []byte) (int, error) {
	var err error

	if len(p) <= m.minBurst {
		// Fast path
		err = m.waitN(m.ctx, len(p))
		if err != nil {
			return 0, err
		}

		return m.r.Read(p)
	}

	var (
		n         int = 0
		offset    int = 0
		remaining int = len(p)
	)

	for remaining > m.minBurst {
		err = m.waitN(m.ctx, m.minBurst)
		if err != nil {
			return offset, err
		}

		n, err = m.r.Read(p[offset : offset+m.minBurst])
		offset += n
		remaining -= n
		if err != nil {
			return offset, err
		}
	}

	if remaining > 0 {
		err = m.waitN(m.ctx, remaining)
		if err != nil {
			return offset, err
		}

		n, err = m.r.Read(p[offset : offset+remaining])
		offset += n
		if err != nil {
			return offset, err
		}
	}

	return offset, nil
}

func (m *MultiReader) waitN(ctx context.Context, n int) error {
	var reservationsArray [8]*rate.Reservation
	reservations := reservationsArray[:0]
	now := time.Now()

	for i := range m.limiters {
		reservations = append(reservations, m.limiters[i].ReserveN(now, n))
	}

	now = time.Now()

	delay := reservations[0].DelayFrom(now)
	for i := range reservations {
		reservationDelay := reservations[i].DelayFrom(now)
		if reservationDelay > delay {
			delay = reservationDelay
		}
	}

	timer := time.NewTimer(delay)

	select {
	case <-timer.C:
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	}

	return nil
}

func (m *MultiReader) ResetReader(rd io.Reader) {
	m.r = rd
}

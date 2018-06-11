package ratedreader

import (
	"context"
	"io"

	"golang.org/x/time/rate"
)

// DefaultBurstSize is the default size of bursts.
var DefaultBurstSize int = 32 * 1024

var _ io.Reader = &Reader{}

type Reader struct {
	r       io.Reader
	limiter rate.Limiter
	burst   int
	ctx     context.Context
}

func NewReader(rd io.Reader, limit rate.Limit) *Reader {
	return NewReaderBurst(rd, limit, DefaultBurstSize)
}

func NewReaderBurst(rd io.Reader, limit rate.Limit, burst int) *Reader {
	return &Reader{
		r:       rd,
		limiter: *rate.NewLimiter(limit, burst),
		burst:   burst,
		ctx:     context.Background(),
	}
}

func (r *Reader) Read(p []byte) (int, error) {
	var err error

	if len(p) <= r.burst {
		// Fast path
		err = r.limiter.WaitN(r.ctx, len(p))
		if err != nil {
			return 0, err
		}

		return r.r.Read(p)
	}

	var (
		n         int = 0
		offset    int = 0
		remaining int = len(p)
	)

	for remaining > r.burst {
		err = r.limiter.WaitN(r.ctx, r.burst)
		if err != nil {
			return offset, err
		}

		n, err = r.r.Read(p[offset : offset+r.burst])
		offset += n
		remaining -= n
		if err != nil {
			return offset, err
		}
	}

	if remaining > 0 {
		err = r.limiter.WaitN(r.ctx, remaining)
		if err != nil {
			return 0, err
		}

		n, err = r.r.Read(p[offset : offset+remaining])
		offset += n
		if err != nil {
			return offset, err
		}
	}

	return offset, nil
}

func (r *Reader) ResetReader(rd io.Reader) {
	r.r = rd
}

func (r *Reader) SetLimit(newLimit rate.Limit) {
	r.limiter.SetLimit(newLimit)
}

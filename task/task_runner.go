package task

import (
	"context"
	"time"
)

// TaskRunner ...
//
type Runner interface {
	Run(ctx context.Context) error

	// Only vals the Runner should use to adjust their run
	SetStart(time.Time)
	SetEnd(time.Time)
}

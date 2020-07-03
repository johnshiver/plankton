package task

import (
	"context"
)

// TaskRunner ...
//
type Runner interface {
	Run(ctx context.Context) error

	// TODO: should implement json unmarshal requirements
	Serialize(string)
	Deserialize() string
}

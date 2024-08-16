package status

import "context"

type Checker interface {
	Status(context.Context) error
}

type logger interface {
	Printf(format string, args ...any)
}

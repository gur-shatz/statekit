package statekit

import "time"

type stateOptions struct {
	importance Importance
	help       string
	now        clock
}

// Option configures built-in state objects.
type Option func(*stateOptions)

func defaultOptions() stateOptions {
	return stateOptions{importance: Important}
}

// WithImportance sets how a state contributes to aggregate parents.
func WithImportance(i Importance) Option {
	return func(o *stateOptions) {
		o.importance = i
	}
}

// WithClock overrides timekeeping. It is mostly useful for tests.
func WithClock(now func() time.Time) Option {
	return func(o *stateOptions) {
		o.now = now
	}
}

// WithHelp attaches a free-form description to the state that surfaces
// in display documents under "help". Useful for explaining what the
// state means in operational terms.
func WithHelp(help string) Option {
	return func(o *stateOptions) {
		o.help = help
	}
}

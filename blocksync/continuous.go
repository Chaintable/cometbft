package blocksync

// ReactorOption configures a block sync reactor.
type ReactorOption func(*Reactor)

// ReactorContinuousMode keeps non-validator nodes in block sync after catching up.
func ReactorContinuousMode(enabled bool) ReactorOption {
	return func(r *Reactor) { r.continuous = enabled }
}

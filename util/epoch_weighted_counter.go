package counters

// EpochWeightedCounters keeps positional counts in fixed, epoch-aligned time
// windows and reports smoothed values by linearly interpolating between the
// previous window's totals and the current window's totals.
//
// Time is divided into buckets of length span, aligned to Unix epoch time.
// For example, with span=60, buckets start at exact minute boundaries:
//
//	00:00:00
//	00:01:00
//	00:02:00
//	...
//
// Add records values into the bucket containing the given timestamp.
//
// CurrentValue writes a weighted blend of the previous bucket and the current
// bucket into dst, based on how far timestamp is into the current bucket:
//
//   - at the start of a bucket: 100% previous, 0% current
//   - halfway through a bucket: 50% previous, 50% current
//   - near the end of a bucket: near 0% previous, near 100% current
//
// The formula is:
//
//	prev*(span-elapsed)/span + curr*elapsed/span
//
// This is useful for smoothing bucketed counts across time boundaries.
//
// CurrentValue is not a precise rolling-window counter over the last span
// seconds. For the common sliding-window estimate, use CurrentWindowValue,
// which computes:
//
//	curr + prev*(span-elapsed)/span
//
// The counter intentionally has no type safety for dimensions. Slots are just
// positions in a slice; callers are responsible for using the same order and
// passing slices with at least Len elements. Extra input/output elements are
// ignored.
//
// Concurrency: this type is not goroutine-safe. Callers must serialize access
// to Add and CurrentValue, for example by keeping the counter inside a service
// goroutine or protecting it with a mutex. The counter intentionally avoids
// internal synchronization so ownership and locking policy stay external.
type EpochWeightedCounters struct {
	curr []int64 // counts for the current epoch-aligned bucket
	prev []int64 // counts for the immediately previous bucket

	span uint32 // bucket size in seconds

	// start is the Unix timestamp, in seconds, of the start of the current
	// bucket.
	//
	// For example, if span=60 and the current timestamp is 123456789,
	// then start will be timestamp rounded down to the nearest minute.
	start uint32
}

// NewEpochWeightedCounters creates counters with the given bucket span.
//
// span is in seconds and must be greater than zero.
// now is used to initialize the current epoch-aligned bucket.
// width is the number of positional counters maintained in each bucket.
func NewEpochWeightedCounters(span uint32, now uint32, width int) *EpochWeightedCounters {
	if span == 0 {
		panic("span must be greater than zero")
	}
	if width < 0 {
		panic("width must be greater than or equal to zero")
	}

	c := &EpochWeightedCounters{
		curr: make([]int64, width),
		prev: make([]int64, width),
		span: span,
	}
	c.start = bucketStart(now, span)
	return c
}

// Len returns the number of positional counters maintained by c.
func (c *EpochWeightedCounters) Len() int {
	return len(c.curr)
}

// Add records values into the bucket containing timestamp.
//
// This implementation assumes timestamps are generally monotonic or near-real-time.
// If Add is called with a timestamp older than the current bucket, the value is
// ignored because this counter only keeps the current and immediately previous
// buckets.
//
// values must contain at least Len elements. Extra elements are ignored.
func (c *EpochWeightedCounters) Add(timestamp uint32, values []int64) {
	if !c.rotate(timestamp) {
		return
	}

	for i := range c.curr {
		c.curr[i] += values[i]
	}
}

// CurrentValue writes the interpolated values between the previous and current
// buckets for the supplied timestamp into dst.
//
// Let:
//
//	elapsed = timestamp - currentBucketStart
//
// Then:
//
//	previous weight = (span - elapsed) / span
//	current weight  = elapsed / span
//
// So the returned value is:
//
//	prev*previousWeight + curr*currentWeight
//
// Integer math is used, so the result is truncated toward zero.
//
// dst must contain at least Len elements. Extra elements are left unchanged.
func (c *EpochWeightedCounters) CurrentValue(timestamp uint32, dst []int64) {
	if !c.rotate(timestamp) {
		for i := range c.curr {
			dst[i] = 0
		}
		return
	}

	elapsed := timestamp - c.start

	for i := range c.curr {
		curr := c.curr[i]
		prev := c.prev[i]
		dst[i] = (prev*int64(c.span-elapsed) +
			curr*int64(elapsed)) / int64(c.span)
	}
}

// CurrentWindowValue writes an estimated current-window value into dst.
//
// Unlike CurrentValue, this uses the common sliding-window approximation:
//
//	curr + prev*(span-elapsed)/span
//
// That makes events in the current bucket count immediately, while the
// previous bucket fades out as timestamp advances through the current bucket.
func (c *EpochWeightedCounters) CurrentWindowValue(timestamp uint32, dst []int64) {
	if !c.rotate(timestamp) {
		for i := range c.curr {
			dst[i] = 0
		}
		return
	}

	elapsed := timestamp - c.start

	for i := range c.curr {
		curr := c.curr[i]
		prev := c.prev[i]
		dst[i] = curr + prev*int64(c.span-elapsed)/int64(c.span)
	}
}

// rotate advances the counter's buckets so that timestamp falls inside the
// current bucket.
//
// It returns false when timestamp is older than the currently tracked bucket.
// In that case, the timestamp is stale and cannot be represented because the
// counter only keeps two buckets: current and previous.
//
// rotate must be called by code that already owns exclusive access to c.
func (c *EpochWeightedCounters) rotate(timestamp uint32) bool {
	newStart := bucketStart(timestamp, c.span)
	start := c.start

	// timestamp is inside the current bucket.
	if newStart == start {
		return true
	}

	// Timestamp is older than the current bucket. We do not rotate backwards.
	if newStart < start {
		return false
	}

	c.start = newStart

	switch {
	case newStart == start+c.span:
		// We moved forward by exactly one bucket.
		// The old current bucket becomes the previous bucket.
		for i := range c.curr {
			c.prev[i] = c.curr[i]
			c.curr[i] = 0
		}

	default:
		// We skipped more than one full bucket.
		// Both existing buckets are stale, so reset them.
		for i := range c.curr {
			c.prev[i] = 0
			c.curr[i] = 0
		}
	}

	return true
}

// bucketStart returns timestamp rounded down to the nearest epoch-aligned bucket.
func bucketStart(timestamp uint32, span uint32) uint32 {
	return timestamp - timestamp%span
}

package demo

import (
	"context"
	"time"
)

var MaxCount = 100
var BaseDuration = 100 // Base duration in milliseconds

// │ Input (l)   │ Duration(ms) │
// ├─────────────┼──────────────┤
// │ 0           │ 100          │
// │ 10          │ 102          │
// │ 25          │ 105          │
// │ 50          │ 110          │
// │ 75          │ 115          │
// │ 100         │ 120          │ **** Increase Point ****
// │ 101         │ 121          │
// │ 110         │ 137          │
// │ 150         │ 205          │
// │ 200         │ 290          │
// │ 300         │ 460          │
// │ 500         │ 800          │
// │ 1000        │ 1650         │
// │ 2000        │ 3250         │
// │ 5000        │ 8450         │
func getDuration(l int) int {
	baseDuration := BaseDuration
	baseDuration += int((float64(l) * 0.2))

	if l > MaxCount {
		baseDuration += int((float64(l-MaxCount) * 1.5))
	}

	return baseDuration
}

// Sleep simulates processing time based on the input length l.
func Sleep(ctx context.Context, l int) {
	duration := time.Duration(getDuration(l)) * time.Millisecond

	select {
	case <-ctx.Done():
		return
	case <-time.After(duration):
		return
	}
}

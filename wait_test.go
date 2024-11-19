package wkafka

import (
	"testing"
	"time"
)

func Test_waitRetry_CurrentInterval(t *testing.T) {
	t.Skip("manual next function testing")

	w := newWaitRetry(10*time.Second, 15*time.Minute)

	if got := w.CurrentInterval(); got != 1*time.Second {
		t.Errorf("CurrentInterval() = %v, want %v", got, 1*time.Second)
	}

	w.next()
	w.next()

	if got := w.CurrentInterval(); got != 2*time.Second {
		t.Errorf("CurrentInterval() = %v, want %v", got, 2*time.Second)
	}
}

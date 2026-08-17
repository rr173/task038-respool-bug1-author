package respool

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProbeCanceledHeadWakesNextWaiter(t *testing.T) {
	p, err := New(2)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	hold, err := p.Acquire(context.Background(), 1, time.Minute, PriorityNormal)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	aCh := acquireAsync(t, p, ctx, 2, time.Minute, PriorityNormal)
	waitForWaiters(t, p, 1)
	bCh := acquireAsync(t, p, context.Background(), 1, time.Minute, PriorityNormal)
	waitForWaiters(t, p, 2)

	cancel()
	select {
	case r := <-aCh:
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("head waiter error = %v", r.err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled head waiter did not return")
	}

	// One unit is already available. Once the canceled head is removed, B must
	// be granted without requiring another release or reclaim event.
	select {
	case r := <-bCh:
		if r.err != nil || r.l == nil || r.l.Weight != 1 {
			t.Fatalf("next waiter result = %+v", r)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("next waiter was not woken after head cancellation")
	}
	_ = hold
}

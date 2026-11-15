package leaktest

import (
	"runtime"
	"testing"
	"time"
)

// Check fails t if, once t and its other cleanups have finished, more
// goroutines run than when Check was called. A goroutine may take a
// moment to exit after its owner stops it, so Check waits up to five
// seconds before it reports, printing every goroutine's stack. Call it
// first in a test, so its cleanup runs last, and never in a test that
// runs in parallel with others.
func Check(t testing.TB) {
	t.Helper()
	before := runtime.NumGoroutine()
	t.Cleanup(func() {
		deadline := time.Now().Add(5 * time.Second)
		for runtime.NumGoroutine() > before {
			if time.Now().After(deadline) {
				stacks := make([]byte, 1<<20)
				stacks = stacks[:runtime.Stack(stacks, true)]
				t.Errorf("%d goroutines still running after the test:\n%s", runtime.NumGoroutine()-before, stacks)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

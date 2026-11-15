// Package leaktest checks that a test leaves no goroutines running.
//
// The runtime, the transport, and the binaries start goroutines, and each
// goroutine must stop when its owner stops. Their tests call Check, which
// compares how many goroutines run when a test starts with how many are
// left once its cleanup has run.
package leaktest

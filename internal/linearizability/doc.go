// Package linearizability checks recorded client histories with Porcupine.
//
// It holds the key-value model, partitioned by key so that checks stay
// fast, and the recorder that the simulator and the chaos test use to log
// each operation's call and return times.
package linearizability

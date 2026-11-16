// Package linearizability checks recorded client histories with
// Porcupine.
//
// It holds the key-value model, partitioned by key so that checks stay
// fast, and the Recorder that the simulator, and later the chaos test,
// use to log each operation's call and return times. An operation whose
// outcome its client never learned stays open to the end of the history,
// where the model accepts any result for it. An illegal history is
// written out as Porcupine's visualization.
package linearizability

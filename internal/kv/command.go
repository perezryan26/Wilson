package kv

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// A command is laid out as
//
//	[version u8][op u8][request u64][session u64][seq u64][key length u32][key][value]
//
// in little-endian byte order, with the value running to the end. Every
// command has every field, and those its op does not use are zero. A
// version this code does not know is refused, so every server of a
// cluster must run the same version (docs/adr/0007-client-sessions.md).
const commandHeaderSize = 1 + 1 + 8 + 8 + 8 + 4

// commandVersion is the version of the layout above.
const commandVersion = 1

// Op says what a command does.
type Op uint8

// The ops. Zero is none of them, so a run of zero bytes never reads as a
// command.
const (
	// OpRegister starts a client session. The session's ID is the log
	// index of the command, so no two sessions ever share one (§8).
	OpRegister Op = 1
	// OpGet reads a key's value through the log.
	OpGet Op = 2
	// OpPut sets a key's value.
	OpPut Op = 3
	// OpDelete removes a key.
	OpDelete Op = 4
)

// Command is one operation on the store, as a log entry carries it.
type Command struct {
	Op Op
	// Request identifies the command to the server that proposed it,
	// which waits for its result. The store only copies it into the
	// command's Result.
	Request uint64
	// Session and Seq name, for Put and Delete, the session the write
	// belongs to and the write's number in that session, from 1. A write
	// whose number the session has already applied is answered from the
	// session's record instead of running again (§8).
	Session uint64
	Seq     uint64
	Key     string // Get, Put, and Delete
	Value   []byte // Put
}

// Encode returns the command's bytes.
func (c Command) Encode() []byte {
	le := binary.LittleEndian
	b := make([]byte, 0, commandHeaderSize+len(c.Key)+len(c.Value))
	b = append(b, commandVersion, byte(c.Op))
	b = le.AppendUint64(b, c.Request)
	b = le.AppendUint64(b, c.Session)
	b = le.AppendUint64(b, c.Seq)
	b = le.AppendUint32(b, uint32(len(c.Key)))
	b = append(b, c.Key...)
	return append(b, c.Value...)
}

// DecodeCommand decodes a command from its bytes. It refuses bytes that
// do not fit the layout, an unknown version, and an unknown op.
func DecodeCommand(b []byte) (Command, error) {
	if len(b) < commandHeaderSize {
		return Command{}, fmt.Errorf("kv: a %d-byte command, shorter than the %d-byte header", len(b), commandHeaderSize)
	}
	if b[0] != commandVersion {
		return Command{}, fmt.Errorf("kv: command version %d, want %d", b[0], commandVersion)
	}
	op := Op(b[1])
	if op < OpRegister || op > OpDelete {
		return Command{}, fmt.Errorf("kv: unknown op %d", op)
	}
	le := binary.LittleEndian
	keyEnd := uint64(commandHeaderSize) + uint64(le.Uint32(b[26:]))
	if keyEnd > uint64(len(b)) {
		return Command{}, fmt.Errorf("kv: a key that runs %d bytes past the command's end", keyEnd-uint64(len(b)))
	}
	return Command{
		Op:      op,
		Request: le.Uint64(b[2:]),
		Session: le.Uint64(b[10:]),
		Seq:     le.Uint64(b[18:]),
		Key:     string(b[commandHeaderSize:keyEnd]),
		Value:   bytes.Clone(b[keyEnd:]),
	}, nil
}

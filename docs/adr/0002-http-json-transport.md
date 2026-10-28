# ADR-0002: HTTP/1.1 and JSON transport

- Status: Accepted
- Date: 2026-10-27

## Context

Servers exchange RequestVote, AppendEntries, and InstallSnapshot messages, and clients need an API for Get, Put, and Delete. The project is standard library first, and a runtime dependency needs its own ADR. While debugging a live cluster, it helps to read and send messages by hand.

The options:

- **gRPC with Protocol Buffers.** Typed, compact, and streaming, but it needs code generation and two runtime dependencies (`google.golang.org/grpc` and `google.golang.org/protobuf`).
- **`net/rpc` with gob.** In the standard library, but the package is frozen, gob is a Go-only binary format that is hard to inspect, and its call-and-reply shape fits the core's message model poorly (see below).
- **Raw TCP with custom framing.** Fast, but framing, reconnection, and timeouts would all be ours to write and test.
- **HTTP/1.1 and JSON with `net/http` and `encoding/json`.** Standard library, readable on the wire, usable with `curl`, and one stack serves both node-to-node and client traffic.

## Decision

Node-to-node messages and the client API both use HTTP/1.1 and JSON from `net/http` and `encoding/json`.

- **Raft traffic is one-way.** A node sends each `raft.Message` as its own POST to the peer's message endpoint, and the peer answers `204 No Content` once the message is queued for its core. A Raft-level response, such as a granted vote, travels back later as its own POST. This mirrors the core (ADR-0001), where a response is just another message, released through `Ready` only after the state it reflects is durable. The transport never holds an HTTP request open across a disk write, and it never matches replies to requests.
- **Loss over blocking.** Each peer has one sender goroutine fed by a bounded queue. A message that arrives when the queue is full is dropped, and every request has a timeout. Raft already tolerates lost messages and recovers through heartbeats and elections, so a slow or dead peer costs dropped messages, never a stalled node.
- **Client API.** `GET`, `PUT`, and `DELETE` on `/v1/kv/{key}`, routed with the method and wildcard patterns of `net/http`'s `ServeMux`, with JSON bodies. A follower answers with a hint naming the leader.
- **Behind an interface.** The runtime sends through a `Transport` interface in `internal/transport` whose methods use only `raft` and standard-library types. gRPC, or anything else, can replace HTTP later without touching the core or the runtime.

## Consequences

- There is no code generation and no dependency, and a node can be inspected or prodded with `curl`.
- JSON is larger and slower to encode than a binary format, and `[]byte` fields (commands and snapshot chunks) travel as base64, about a third larger. Clarity beats performance here, and the snapshot chunk size is configurable.
- One-way messages take twice as many HTTP requests as call-and-reply RPC, and each message is its own request with no batching. Keep-alive connections make that cheap on a local network; batching is a later performance question.
- The message decoder reads input from the network, so it gets a fuzz target, and request bodies are size-limited.
- There is no TLS or authentication (a non-goal), so anyone who can reach the port can send Raft messages. The README lists this under Limitations.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/perezryan26/wilson/internal/storage"
	"github.com/perezryan26/wilson/internal/transport"
	"github.com/perezryan26/wilson/node"
	"github.com/perezryan26/wilson/raft"
)

// options are kvnode's settings, from its flags.
type options struct {
	id           raft.NodeID
	peers        map[raft.NodeID]string // every server's address, this one's included
	listen       string
	dataDir      string
	preset       string
	compactEvery int
}

func main() {
	os.Exit(kvnode(os.Args[1:]))
}

// kvnode runs a server with the flags in args until SIGINT or SIGTERM, or
// until its storage fails, and returns the exit code: 0 after a graceful
// shutdown, 1 after a failure, 2 for bad flags.
func kvnode(args []string) int {
	o, err := parseFlags(args)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kvnode:", err)
		return 2
	}
	var running atomic.Pointer[node.Node]
	logger := slog.New(statusHandler{slog.NewTextHandler(os.Stderr, nil), &running}).With("node", uint64(o.id))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, o, logger, &running); err != nil {
		logger.Error("kvnode: stopped", "err", err)
		return 1
	}
	logger.Info("kvnode: stopped")
	return 0
}

// parseFlags returns the options args set, or an error naming the first
// flag that is missing or wrong.
func parseFlags(args []string) (options, error) {
	fset := flag.NewFlagSet("kvnode", flag.ContinueOnError)
	id := fset.Uint64("id", 0, "this server's `ID`, one of those in -peers")
	peers := fset.String("peers", "", "every server in the cluster, this one included, as `id=host:port,...`")
	listen := fset.String("listen", "", "the `address` to listen on for peers and clients (default: this server's address in -peers)")
	dataDir := fset.String("data", "", "the `directory` for this server's log and snapshots; created if missing, but its parent must exist")
	preset := fset.String("preset", "default", "the timing `preset`: default (10 ms ticks, 50 ms heartbeats, 300-600 ms election timeouts)")
	compactEvery := fset.Int("compact-every", 1000, "snapshot the state machine and compact the log after this many `entries`; 0 never does")
	if err := fset.Parse(args); err != nil {
		return options{}, err
	}
	o := options{id: raft.NodeID(*id), listen: *listen, dataDir: *dataDir, preset: *preset, compactEvery: *compactEvery}
	var err error
	if o.peers, err = parsePeers(*peers); err != nil {
		return options{}, err
	}
	switch {
	case fset.NArg() > 0:
		return options{}, fmt.Errorf("unexpected arguments %q", fset.Args())
	case o.peers[o.id] == "":
		return options{}, fmt.Errorf("-id %d is not one of the servers in -peers", o.id)
	case o.dataDir == "":
		return options{}, errors.New("-data is required")
	case o.preset != "default":
		return options{}, fmt.Errorf("-preset %q is not a preset; the one preset is default", o.preset)
	case o.compactEvery < 0:
		return options{}, fmt.Errorf("-compact-every %d is negative", o.compactEvery)
	}
	if o.listen == "" {
		o.listen = o.peers[o.id]
	}
	return o, nil
}

// parsePeers parses -peers: comma-separated id=host:port pairs, with IDs
// from 1 and no ID twice.
func parsePeers(s string) (map[raft.NodeID]string, error) {
	peers := map[raft.NodeID]string{}
	for _, pair := range strings.Split(s, ",") {
		idText, addr, ok := strings.Cut(strings.TrimSpace(pair), "=")
		id, err := strconv.ParseUint(idText, 10, 64)
		switch {
		case !ok || err != nil || id == 0 || addr == "":
			return nil, fmt.Errorf("-peers entry %q is not id=host:port with an ID from 1", pair)
		case peers[raft.NodeID(id)] != "":
			return nil, fmt.Errorf("-peers names server %d twice", id)
		}
		peers[raft.NodeID(id)] = addr
	}
	return peers, nil
}

// run starts the server and serves until ctx ends or the node stops on its
// own, which only a storage error makes it do. Then it shuts down.
func run(ctx context.Context, o options, logger *slog.Logger, running *atomic.Pointer[node.Node]) error {
	if err := createDataDir(o.dataDir); err != nil {
		return err
	}
	st, err := storage.OpenFileStorage(storage.OSFS{}, o.dataDir, storage.FileOptions{Logger: logger})
	if err != nil {
		return err
	}
	defer func() {
		if err := st.Close(); err != nil {
			logger.Error("kvnode: close storage", "err", err)
		}
	}()
	tr := transport.NewHTTP(o.id, othersOf(o.peers, o.id), transport.HTTPOptions{Logger: logger})
	defer tr.Close()
	cfg, tick := raft.DefaultConfig()
	cfg.ID, cfg.Rand = o.id, rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	for id := range o.peers {
		cfg.Peers = append(cfg.Peers, id)
	}
	sm := &commandLog{}
	n, err := node.Start(node.Config{Raft: cfg, Tick: tick, CompactEvery: o.compactEvery, Logger: logger}, st, tr, sm)
	if err != nil {
		return err
	}
	running.Store(n)
	mux := http.NewServeMux()
	mux.Handle(transport.MessagePath, tr.Handler(n.Step))
	debugAPI{node: n, sm: sm, maxCommand: cfg.MaxAppendSize}.register(mux)
	return serve(ctx, o.listen, n, mux, logger)
}

// othersOf returns peers without server id: the servers id sends to.
func othersOf(peers map[raft.NodeID]string, id raft.NodeID) map[raft.NodeID]string {
	others := map[raft.NodeID]string{}
	for p, addr := range peers {
		if p != id {
			others[p] = addr
		}
	}
	return others
}

// serve serves h at addr until ctx ends, the node stops on its own, or
// serving fails. Then it stops taking requests, lets those under way
// finish, and stops the node. It returns the error that ended it, if one
// did; a storage error comes first, because it means the disk can no
// longer be trusted.
func serve(ctx context.Context, addr string, n *node.Node, h http.Handler, logger *slog.Logger) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Join(err, n.Stop())
	}
	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	logger.Info("kvnode: serving", "addr", ln.Addr().String())
	select {
	case <-ctx.Done():
		logger.Info("kvnode: shutting down")
	case <-n.Done():
	case err = <-served:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if serr := srv.Shutdown(shutdown); serr != nil {
		logger.Warn("kvnode: shut down the HTTP server", "err", serr)
	}
	if stopErr := n.Stop(); stopErr != nil {
		return stopErr
	}
	return err
}

// createDataDir creates dir, whose parent must exist, unless it exists,
// then fsyncs the parent. The directory's entry in its parent must be
// durable before anything in it is: a crash could otherwise lose the
// directory, fsynced files and all. It syncs the parent even when dir
// exists, because the run that created it may have crashed first.
func createDataDir(dir string) error {
	dir = filepath.Clean(dir)
	if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("create the data directory: %w", err)
	}
	if err := (storage.OSFS{}).SyncDir(filepath.Dir(dir)); err != nil {
		return fmt.Errorf("sync the data directory's parent: %w", err)
	}
	return nil
}

// statusHandler adds the node's term and role to every log line, once the
// node runs, so that each line says what the server believed when it was
// written.
type statusHandler struct {
	slog.Handler
	node *atomic.Pointer[node.Node]
}

func (h statusHandler) Handle(ctx context.Context, r slog.Record) error {
	if n := h.node.Load(); n != nil {
		st := n.Status()
		r.AddAttrs(slog.Uint64("term", uint64(st.Term)), slog.String("role", st.Role.String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h statusHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return statusHandler{h.Handler.WithAttrs(attrs), h.node}
}

func (h statusHandler) WithGroup(name string) slog.Handler {
	return statusHandler{h.Handler.WithGroup(name), h.node}
}

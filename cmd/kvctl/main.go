package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/perezryan26/wilson/client"
)

func main() {
	os.Exit(kvctl(os.Args[1:], os.Stdout, os.Stderr))
}

// kvctl runs the command in args, writing its output to stdout and its
// errors to stderr, and returns the exit code: 0 on success, 1 when the
// command failed or found no such key, 2 for bad usage.
func kvctl(args []string, stdout, stderr io.Writer) int {
	fset := flag.NewFlagSet("kvctl", flag.ContinueOnError)
	fset.SetOutput(stderr)
	servers := fset.String("servers", "127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003", "the cluster's servers, as `host:port,...`")
	timeout := fset.Duration("timeout", 10*time.Second, "how long to keep trying before giving up")
	fset.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "usage: kvctl [flags] get KEY | put KEY VALUE | delete KEY | status")
		fset.PrintDefaults()
	}
	if err := fset.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	c := client.New(strings.Split(*servers, ","), client.Options{})
	defer c.Close()
	err := run(ctx, c, strings.Split(*servers, ","), fset.Args(), stdout)
	switch {
	case errors.Is(err, errUsage):
		fset.Usage()
		return 2
	case errors.Is(err, client.ErrUnknownOutcome), errors.Is(err, client.ErrSessionExpired):
		_, _ = fmt.Fprintf(stderr, "kvctl: %v; the write may or may not have been applied\n", err)
		return 1
	case err != nil:
		_, _ = fmt.Fprintln(stderr, "kvctl:", err)
		return 1
	}
	return 0
}

// errUsage is the error for a command kvctl does not know, or one with the
// wrong number of arguments.
var errUsage = errors.New("usage")

// command is one of kvctl's commands: how many arguments it takes, and
// what it does with them.
type command struct {
	args int
	run  func(ctx context.Context, c *client.Client, args []string, stdout io.Writer) error
}

// run carries out the command args name.
func run(ctx context.Context, c *client.Client, servers, args []string, stdout io.Writer) error {
	commands := map[string]command{
		"get":    {1, get},
		"put":    {2, put},
		"delete": {1, del},
		"status": {0, func(ctx context.Context, c *client.Client, _ []string, stdout io.Writer) error {
			return status(ctx, c, servers, stdout)
		}},
	}
	if len(args) == 0 {
		return errUsage
	}
	cmd, ok := commands[args[0]]
	if !ok || len(args)-1 != cmd.args {
		return errUsage
	}
	return cmd.run(ctx, c, args[1:], stdout)
}

// get prints a key's value.
func get(ctx context.Context, c *client.Client, args []string, stdout io.Writer) error {
	value, found, err := c.Get(ctx, args[0])
	switch {
	case err != nil:
		return err
	case !found:
		return fmt.Errorf("no key %q", args[0])
	}
	_, err = fmt.Fprintf(stdout, "%s\n", value)
	return err
}

func put(ctx context.Context, c *client.Client, args []string, _ io.Writer) error {
	return c.Put(ctx, args[0], []byte(args[1]))
}

func del(ctx context.Context, c *client.Client, args []string, _ io.Writer) error {
	existed, err := c.Delete(ctx, args[0])
	if err == nil && !existed {
		err = fmt.Errorf("no key %q", args[0])
	}
	return err
}

// status prints each server's role, term, leader, commitIndex, and
// lastApplied, or why it did not answer. The table is buffered, so a
// failed write surfaces from Flush.
func status(ctx context.Context, c *client.Client, servers []string, stdout io.Writer) error {
	w := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "SERVER\tID\tROLE\tTERM\tLEADER\tCOMMIT\tAPPLIED")
	for _, server := range servers {
		st, err := c.Status(ctx, server)
		if err != nil {
			_, _ = fmt.Fprintf(w, "%s\t-\tunreachable: %v\n", server, err)
			continue
		}
		_, _ = fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%d\t%d\t%d\n", server, st.ID, st.Role, st.Term, st.Leader, st.CommitIndex, st.LastApplied)
	}
	return w.Flush()
}

package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/perezryan26/wilson/internal/kvtest"
	"github.com/perezryan26/wilson/internal/leaktest"
)

// kvctlRun runs kvctl with args against servers and returns its exit code,
// output, and errors.
func kvctlRun(servers string, args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := kvctl(append([]string{"-servers", servers}, args...), &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

// Each command against a cluster: put, get, delete, and status, and the
// exit codes for a missing key and for bad usage.
func TestCommands(t *testing.T) {
	leaktest.Check(t)
	c := kvtest.Start(t, kvtest.Options{Size: 3})
	lead := c.Leader()
	servers := strings.Join(c.Addrs(), ",")
	steps := []struct {
		args   []string
		code   int
		stdout string
		stderr string
	}{
		{[]string{"put", "greeting", "hello"}, 0, "", ""},
		{[]string{"get", "greeting"}, 0, "hello\n", ""},
		{[]string{"delete", "greeting"}, 0, "", ""},
		{[]string{"delete", "greeting"}, 1, "", "no key \"greeting\""},
		{[]string{"get", "greeting"}, 1, "", "no key \"greeting\""},
		{[]string{"status"}, 0, c.Addr(lead), ""},
		{[]string{"get"}, 2, "", "usage"},
		{[]string{"fly"}, 2, "", "usage"},
		{nil, 2, "", "usage"},
	}
	for _, s := range steps {
		code, stdout, stderr := kvctlRun(servers, s.args...)
		if code != s.code || !strings.Contains(stdout, s.stdout) || !strings.Contains(stderr, s.stderr) {
			t.Errorf("kvctl %v: exit %d, output %q, errors %q; want %d, %q, %q", s.args, code, stdout, stderr, s.code, s.stdout, s.stderr)
		}
	}
	_, stdout, _ := kvctlRun(servers, "status")
	if lines := strings.Split(strings.TrimSpace(stdout), "\n"); len(lines) != 4 || strings.Count(stdout, " leader ") != 1 {
		t.Errorf("status printed\n%s\nwant a header and three servers, one of them the leader", stdout)
	}
}

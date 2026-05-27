package manager

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---------- ringBuffer ----------

func TestRingBuffer_Empty(t *testing.T) {
	rb := newRingBuffer(5)
	if got := rb.tail(10); len(got) != 0 {
		t.Errorf("empty tail = %v, want []", got)
	}
}

func TestRingBuffer_BelowCapacity(t *testing.T) {
	rb := newRingBuffer(5)
	rb.add("a")
	rb.add("b")
	rb.add("c")
	got := rb.tail(10)
	want := []string{"a", "b", "c"}
	if !equalSlices(got, want) {
		t.Errorf("tail = %v, want %v", got, want)
	}
}

func TestRingBuffer_WrapsAtCapacity(t *testing.T) {
	rb := newRingBuffer(3)
	for _, s := range []string{"a", "b", "c", "d", "e"} {
		rb.add(s)
	}
	got := rb.tail(10)
	want := []string{"c", "d", "e"}
	if !equalSlices(got, want) {
		t.Errorf("tail = %v, want %v", got, want)
	}
}

func TestRingBuffer_TailClamps(t *testing.T) {
	rb := newRingBuffer(5)
	for _, s := range []string{"a", "b", "c"} {
		rb.add(s)
	}
	if got := rb.tail(0); len(got) != 3 {
		t.Errorf("tail(0) length = %d, want 3 (clamps to size)", len(got))
	}
	if got := rb.tail(2); !equalSlices(got, []string{"b", "c"}) {
		t.Errorf("tail(2) = %v, want [b c]", got)
	}
	if got := rb.tail(100); len(got) != 3 {
		t.Errorf("tail(100) length = %d, want 3 (clamps to size)", len(got))
	}
}

func TestRingBuffer_Concurrent(t *testing.T) {
	rb := newRingBuffer(64)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				rb.add(fmt.Sprintf("w%d-%d", i, j))
			}
		}(i)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = rb.tail(10)
			}
		}()
	}
	wg.Wait()
}

// ---------- Manager.Start / Stop ----------

func writeTestBin(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tinybin.sh")
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// freePort picks an unbound port via :0 then immediately closes the listener,
// so tests can declare a port up front without racing the (now-removed)
// allocator. We dedupe against a process-global set because shell test
// processes don't actually bind the port — without the dedupe, a closed-then-
// reissued ephemeral port can collide with a still-recorded entry in some
// Manager's byPort index from an earlier subtest.
var (
	issuedPortsMu sync.Mutex
	issuedPorts   = map[int]bool{}
)

func freePort(t *testing.T) int {
	t.Helper()
	issuedPortsMu.Lock()
	defer issuedPortsMu.Unlock()
	for try := 0; try < 64; try++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		if !issuedPorts[port] {
			issuedPorts[port] = true
			return port
		}
	}
	t.Fatal("could not allocate a unique freePort after 64 attempts")
	return 0
}

func TestStart_HappyPath(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\necho \"started on $PORT\"\nsleep 30\n")
	m := New(20, 0, "")
	t.Cleanup(func() { stopAll(t, m) })

	port := freePort(t)
	p, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "alpha", Port: port, Gateway: "g1"})
	if err != nil {
		t.Fatal(err)
	}
	if p.Port != port {
		t.Errorf("port = %d, want %d", p.Port, port)
	}
	if p.Status() != StatusRunning {
		t.Errorf("status = %s, want running", p.Status())
	}
	if p.Gateway != "g1" {
		t.Errorf("gateway = %q, want g1", p.Gateway)
	}

	waitFor(t, 2*time.Second, func() bool {
		for _, line := range p.Tail(10) {
			if strings.Contains(line, fmt.Sprintf("started on %d", port)) {
				return true
			}
		}
		return false
	}, "log line with PORT never appeared")
}

func TestStart_LooksUpBySubdomain(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\nsleep 30\n")
	m := New(10, 0, "")
	t.Cleanup(func() { stopAll(t, m) })

	p, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "beta", Port: freePort(t), Gateway: "g"})
	if err != nil {
		t.Fatal(err)
	}
	got, ok := m.GetBySubdomain("beta")
	if !ok || got.ID != p.ID {
		t.Errorf("GetBySubdomain returned %v (ok=%v), want %s", got, ok, p.ID)
	}
}

func TestStart_ValidationErrors(t *testing.T) {
	m := New(10, 0, "")
	t.Cleanup(func() { stopAll(t, m) })

	t.Run("bad subdomain", func(t *testing.T) {
		bin := writeTestBin(t, "#!/bin/sh\nsleep 1\n")
		_, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "Foo_BAD", Port: freePort(t), Gateway: "g"})
		if err == nil || !strings.Contains(err.Error(), "invalid subdomain") {
			t.Errorf("got %v, want invalid subdomain error", err)
		}
	})

	t.Run("invalid port", func(t *testing.T) {
		bin := writeTestBin(t, "#!/bin/sh\nsleep 1\n")
		_, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "ok", Port: 0, Gateway: "g"})
		if err == nil || !strings.Contains(err.Error(), "invalid port") {
			t.Errorf("got %v, want invalid-port error", err)
		}
	})

	t.Run("binary not found", func(t *testing.T) {
		_, err := m.Start(StartOptions{BinaryPath: "/nonexistent/path/bin", Subdomain: "ok", Port: freePort(t), Gateway: "g"})
		if err == nil || !strings.Contains(err.Error(), "binary not found") {
			t.Errorf("got %v, want binary-not-found error", err)
		}
	})

	t.Run("binary not executable", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "noexec")
		if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := m.Start(StartOptions{BinaryPath: path, Subdomain: "ok2", Port: freePort(t), Gateway: "g"})
		if err == nil || !strings.Contains(err.Error(), "no executable bit") {
			t.Errorf("got %v, want no-executable-bit error", err)
		}
	})

	t.Run("binary path is a directory", func(t *testing.T) {
		dir := t.TempDir()
		_, err := m.Start(StartOptions{BinaryPath: dir, Subdomain: "dir", Port: freePort(t), Gateway: "g"})
		if err == nil || !strings.Contains(err.Error(), "is a directory") {
			t.Errorf("got %v, want is-a-directory error", err)
		}
	})

	t.Run("permission denied on stat", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("root bypasses dir permissions, can't exercise EACCES via stat")
		}
		dir := t.TempDir()
		path := filepath.Join(dir, "hidden")
		if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		// 0o000 on the parent dir makes stat of children return EACCES.
		if err := os.Chmod(dir, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
		_, err := m.Start(StartOptions{BinaryPath: path, Subdomain: "perm", Port: freePort(t), Gateway: "g"})
		if err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("got %v, want permission-denied error", err)
		}
	})

	t.Run("bad shebang interpreter", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "badshebang")
		if err := os.WriteFile(path, []byte("#!/no/such/interpreter\n"), 0o755); err != nil {
			t.Fatal(err)
		}
		_, err := m.Start(StartOptions{BinaryPath: path, Subdomain: "shebang", Port: freePort(t), Gateway: "g"})
		if err == nil || !strings.Contains(err.Error(), "interpreter not found") {
			t.Errorf("got %v, want shebang-interpreter error", err)
		}
	})

	t.Run("subdomain collision", func(t *testing.T) {
		bin := writeTestBin(t, "#!/bin/sh\nsleep 30\n")
		_, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "dup", Port: freePort(t), Gateway: "g"})
		if err != nil {
			t.Fatal(err)
		}
		_, err = m.Start(StartOptions{BinaryPath: bin, Subdomain: "dup", Port: freePort(t), Gateway: "g"})
		if err == nil || !strings.Contains(err.Error(), "already in use") {
			t.Errorf("got %v, want subdomain-in-use error", err)
		}
	})

	t.Run("port collision", func(t *testing.T) {
		bin := writeTestBin(t, "#!/bin/sh\nsleep 30\n")
		port := freePort(t)
		_, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "pc1", Port: port, Gateway: "g"})
		if err != nil {
			t.Fatal(err)
		}
		_, err = m.Start(StartOptions{BinaryPath: bin, Subdomain: "pc2", Port: port, Gateway: "g"})
		if err == nil || !strings.Contains(err.Error(), "port") {
			t.Errorf("got %v, want port-collision error", err)
		}
	})

	t.Run("port bound on host", func(t *testing.T) {
		bin := writeTestBin(t, "#!/bin/sh\nsleep 30\n")
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		port := l.Addr().(*net.TCPAddr).Port
		_, err = m.Start(StartOptions{BinaryPath: bin, Subdomain: "bound", Port: port, Gateway: "g"})
		if err == nil || !strings.Contains(err.Error(), "already bound") {
			t.Errorf("got %v, want already-bound error", err)
		}
	})
}

func TestStart_DefaultsWorkingDirToBinaryDir(t *testing.T) {
	// Drop a binary that reports its own CWD; default WorkingDir should be the
	// directory containing the binary, so the printed line should match it.
	dir := t.TempDir()
	bin := filepath.Join(dir, "pwdbin.sh")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\npwd\nsleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	m := New(20, 0, "")
	t.Cleanup(func() { stopAll(t, m) })

	p, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "cwd", Port: freePort(t), Gateway: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if p.WorkingDir != dir {
		t.Errorf("WorkingDir = %q, want %q", p.WorkingDir, dir)
	}

	// macOS /tmp may be a symlink to /private/tmp, so resolve before matching.
	wantResolved, _ := filepath.EvalSymlinks(dir)
	waitFor(t, 2*time.Second, func() bool {
		for _, line := range p.Tail(10) {
			if line == dir || line == wantResolved {
				return true
			}
		}
		return false
	}, "child never logged the expected CWD")
}

func TestStart_ExplicitWorkingDirIsUsed(t *testing.T) {
	binDir := t.TempDir()
	bin := filepath.Join(binDir, "pwdbin.sh")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\npwd\nsleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cwd := t.TempDir() // separate from binDir to prove the override is used
	m := New(20, 0, "")
	t.Cleanup(func() { stopAll(t, m) })

	p, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "ecwd", Port: freePort(t), Gateway: "g", WorkingDir: cwd})
	if err != nil {
		t.Fatal(err)
	}
	if p.WorkingDir != cwd {
		t.Errorf("WorkingDir = %q, want %q", p.WorkingDir, cwd)
	}
	wantResolved, _ := filepath.EvalSymlinks(cwd)
	waitFor(t, 2*time.Second, func() bool {
		for _, line := range p.Tail(10) {
			if line == cwd || line == wantResolved {
				return true
			}
		}
		return false
	}, "child never logged the overridden CWD")
}

func TestStart_WorkingDirValidation(t *testing.T) {
	m := New(20, 0, "")
	t.Cleanup(func() { stopAll(t, m) })
	bin := writeTestBin(t, "#!/bin/sh\nsleep 1\n")

	t.Run("not found", func(t *testing.T) {
		_, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "wd1", Port: freePort(t), Gateway: "g", WorkingDir: "/nonexistent/dir"})
		if err == nil || !strings.Contains(err.Error(), "working_dir not found") {
			t.Errorf("got %v, want working_dir-not-found error", err)
		}
	})

	t.Run("not a directory", func(t *testing.T) {
		_, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "wd2", Port: freePort(t), Gateway: "g", WorkingDir: bin})
		if err == nil || !strings.Contains(err.Error(), "is not a directory") {
			t.Errorf("got %v, want not-a-directory error", err)
		}
	})
}

func TestStart_ExpandsHomePrefix(t *testing.T) {
	// Write the binary inside a temp dir and treat that dir as the base. A
	// "~/<name>" path should resolve to it and start successfully; the stored
	// BinaryPath should be the expanded absolute path so logs / status are
	// debuggable.
	base := t.TempDir()
	path := filepath.Join(base, "tinybin.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nsleep 30\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	m := New(10, 0, base)
	t.Cleanup(func() { stopAll(t, m) })

	p, err := m.Start(StartOptions{BinaryPath: "~/tinybin.sh", Subdomain: "hp", Port: freePort(t), Gateway: "g"})
	if err != nil {
		t.Fatalf("start with ~ prefix failed: %v", err)
	}
	if p.BinaryPath != path {
		t.Errorf("BinaryPath = %q, want expanded %q", p.BinaryPath, path)
	}
}

func TestStart_TildeWithoutBaseLeftLiteral(t *testing.T) {
	// base == "" disables expansion: a literal "~/..." should fail the
	// not-found check rather than getting silently rewritten.
	m := New(10, 0, "")
	t.Cleanup(func() { stopAll(t, m) })

	_, err := m.Start(StartOptions{BinaryPath: "~/nope", Subdomain: "lit", Port: freePort(t), Gateway: "g"})
	if err == nil || !strings.Contains(err.Error(), "binary not found") {
		t.Errorf("got %v, want binary-not-found (no expansion when base is empty)", err)
	}
}

func TestExpandBinaryPath(t *testing.T) {
	cases := []struct {
		in, base, want string
	}{
		{"~", "/h", "/h"},
		{"~/foo", "/h", "/h/foo"},
		{"~/a/b", "/h", "/h/a/b"},
		{"~foo", "/h", "~foo"},       // ~user form: unsupported, passthrough
		{"/abs/path", "/h", "/abs/path"}, // absolute: untouched
		{"rel/path", "/h", "rel/path"},   // relative: untouched
		{"~/foo", "", "~/foo"},           // empty base: disabled
		{"", "/h", ""},                   // empty path: passthrough
	}
	for _, c := range cases {
		if got := expandBinaryPath(c.in, c.base); got != c.want {
			t.Errorf("expandBinaryPath(%q, %q) = %q, want %q", c.in, c.base, got, c.want)
		}
	}
}

func TestStop_FlipsStatusAndFreesPort(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\nsleep 30\n")
	m := New(10, 0, "")
	t.Cleanup(func() { stopAll(t, m) })

	port := freePort(t)
	p, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "p1", Port: port, Gateway: "g"})
	if err != nil {
		t.Fatal(err)
	}

	if err := m.Stop(p.ID); err != nil {
		t.Fatal(err)
	}
	if p.Status() != StatusStopped {
		t.Errorf("after stop, status = %s, want stopped", p.Status())
	}

	// The same port should now be reusable for a new process.
	if _, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "p2", Port: port, Gateway: "g"}); err != nil {
		t.Errorf("starting on freed port failed: %v", err)
	}
}

func TestHealth_UnhealthyWhenProcessDoesNotBind(t *testing.T) {
	// Shorten the probe window so the test doesn't sit for 5s. Mutating the
	// Manager's fields *before* Start is race-safe (the `go` statement
	// establishes a happens-before to the probe goroutine).
	bin := writeTestBin(t, "#!/bin/sh\nsleep 30\n")
	m := New(10, 0, "")
	m.probeTimeout = 400 * time.Millisecond
	m.probeInterval = 50 * time.Millisecond
	t.Cleanup(func() { stopAll(t, m) })

	p, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "unhealthy", Port: freePort(t), Gateway: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if p.Health() != HealthStarting {
		t.Errorf("initial health = %s, want starting", p.Health())
	}

	waitFor(t, 2*time.Second, func() bool {
		return p.Health() == HealthUnhealthy
	}, "health never flipped to unhealthy")
}

func TestProcessNaturalExitFlipsStatus(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\nexit 0\n")
	m := New(10, 0, "")
	t.Cleanup(func() { stopAll(t, m) })

	p, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "exit", Port: freePort(t), Gateway: "g"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, func() bool {
		return p.Status() == StatusStopped
	}, "process never flipped to stopped after natural exit")

	if _, ok := m.GetBySubdomain("exit"); ok {
		t.Errorf("subdomain entry should be removed after exit")
	}
}

func TestJanitor_SweepsStoppedAfterTTL(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\nexit 0\n")
	ttl := 50 * time.Millisecond
	m := New(10, ttl, "") // ttl > 0 also spawns the janitor goroutine
	t.Cleanup(func() { stopAll(t, m) })

	p, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "gone", Port: freePort(t), Gateway: "g"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		return p.Status() == StatusStopped
	}, "process never reached stopped status")

	// Sleep past the TTL, then trigger a sweep directly (don't wait for the
	// 1-minute janitor tick). Verifies the policy without timing flake.
	time.Sleep(ttl + 50*time.Millisecond)
	m.sweepStopped()

	if _, ok := m.Get(p.ID); ok {
		t.Errorf("stopped process still present after sweep")
	}
}

func TestJanitor_KeepsStoppedWhenTTLZero(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\nexit 0\n")
	m := New(10, 0, "") // ttl=0: janitor disabled, entries kept forever; "" disables ~ expansion
	t.Cleanup(func() { stopAll(t, m) })

	p, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "keep", Port: freePort(t), Gateway: "g"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		return p.Status() == StatusStopped
	}, "process never reached stopped status")

	// sweep is a guarded no-op when ttl=0 — calling it should not drop anything.
	m.sweepStopped()
	if _, ok := m.Get(p.ID); !ok {
		t.Errorf("process unexpectedly removed when ttl=0")
	}
}

func TestRestart_NewIDSameSubdomainAndPort(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\nsleep 30\n")
	m := New(10, 0, "")
	t.Cleanup(func() { stopAll(t, m) })

	port := freePort(t)
	p, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "rs", Port: port, Gateway: "g"})
	if err != nil {
		t.Fatal(err)
	}
	oldID := p.ID

	p2, err := m.Restart(oldID)
	if err != nil {
		t.Fatal(err)
	}
	if p2.ID == oldID {
		t.Errorf("restart should produce a new id, got same %q", p2.ID)
	}
	if p2.Subdomain != "rs" || p2.Port != port || p2.Gateway != "g" {
		t.Errorf("restart did not preserve opts: %+v", p2)
	}
	if p2.Status() != StatusRunning {
		t.Errorf("new process status = %s, want running", p2.Status())
	}

	// Old entry stays reachable by its original id (memory documents this is
	// intentional so logs + exit info remain queryable).
	if oldP, ok := m.Get(oldID); !ok {
		t.Errorf("old process %q should still be reachable after restart", oldID)
	} else if oldP.Status() != StatusStopped {
		t.Errorf("old process status = %s, want stopped", oldP.Status())
	}
}

func TestRestart_NotFound(t *testing.T) {
	m := New(10, 0, "")
	t.Cleanup(func() { stopAll(t, m) })
	_, err := m.Restart("nonexistent")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("got %v, want not-found error", err)
	}
}

func TestStopAll(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\nsleep 30\n")
	m := New(10, 0, "")

	for i := 0; i < 3; i++ {
		_, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: fmt.Sprintf("s%d", i), Port: freePort(t), Gateway: "g"})
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m.StopAll(ctx)
	for _, p := range m.List() {
		if p.Status() != StatusStopped {
			t.Errorf("process %s status = %s, want stopped", p.ID, p.Status())
		}
	}
}

// ---------- helpers ----------

func stopAll(t *testing.T, m *Manager) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m.StopAll(ctx)
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(msg)
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

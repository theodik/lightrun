package manager

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStore_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "state.json")
	fs := NewFileStore(path, nil)
	want := []StartOptions{
		{
			Command:     "npm run dev",
			Name:        "Dev",
			Description: "Development server",
			Subdomain:   "a",
			Port:        1000,
			Gateway:     "g1",
			WorkingDir:  "/srv",
			Env:         map[string]string{"K": "v"},
		},
		{
			BinaryPath: "/srv/b",
			Name:       "Worker",
			Subdomain:  "b",
			Port:       1001,
			Gateway:    "g2",
		},
	}
	if err := fs.Save(want); err != nil {
		t.Fatal(err)
	}

	got, err := fs.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("loaded %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Command != want[i].Command ||
			got[i].BinaryPath != want[i].BinaryPath ||
			got[i].Name != want[i].Name ||
			got[i].Description != want[i].Description ||
			got[i].Subdomain != want[i].Subdomain ||
			got[i].Port != want[i].Port ||
			got[i].Gateway != want[i].Gateway ||
			got[i].WorkingDir != want[i].WorkingDir {
			t.Errorf("entry %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	if got[0].Env["K"] != "v" {
		t.Errorf("env not round-tripped: %v", got[0].Env)
	}
}

func TestFileStore_LoadMissingIsEmpty(t *testing.T) {
	fs := NewFileStore(filepath.Join(t.TempDir(), "absent.json"), nil)
	got, err := fs.Load()
	if err != nil {
		t.Fatalf("missing file should not error, got %v", err)
	}
	if len(got) != 0 {
		t.Errorf("missing file should load empty, got %v", got)
	}
}

func TestFileStore_LoadCorruptErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileStore(path, nil).Load(); err == nil {
		t.Error("corrupt state file should error on load")
	}
}

func TestManager_NamedCommandPersistsAndStopKeepsRegistration(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\nsleep 30\n")
	path := filepath.Join(t.TempDir(), "state.json")
	m := New(10, 0, "")
	m.SetStore(NewFileStore(path, nil))
	t.Cleanup(func() { stopAll(t, m) })

	p, err := m.Start(StartOptions{
		BinaryPath: bin,
		Name:       "Keep",
		Subdomain:  "keep",
		Port:       freePort(t),
		Gateway:    "g",
	})
	if err != nil {
		t.Fatal(err)
	}
	entries := loadState(t, path)
	if len(entries) != 1 || entries[0].Subdomain != "keep" || entries[0].Name != "Keep" {
		t.Fatalf("after start, state = %+v, want one named keep entry", entries)
	}

	if err := m.Stop(p.ID); err != nil {
		t.Fatal(err)
	}
	entries = loadState(t, path)
	if len(entries) != 1 || entries[0].Subdomain != "keep" {
		t.Errorf("after stop, named command should stay registered, got %+v", entries)
	}
}

func TestManager_AdhocProcessIsNotPersisted(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\nsleep 30\n")
	path := filepath.Join(t.TempDir(), "state.json")
	m := New(10, 0, "")
	m.SetStore(NewFileStore(path, nil))
	t.Cleanup(func() { stopAll(t, m) })

	if _, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "adhoc", Port: freePort(t), Gateway: "g"}); err != nil {
		t.Fatal(err)
	}
	if entries := loadState(t, path); len(entries) != 0 {
		t.Errorf("ad-hoc process should not persist, got %+v", entries)
	}
}

func TestManager_NamedCrashKeepsRegistration(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\nexit 0\n")
	path := filepath.Join(t.TempDir(), "state.json")
	m := New(10, 0, "")
	m.SetStore(NewFileStore(path, nil))
	t.Cleanup(func() { stopAll(t, m) })

	p, err := m.Start(StartOptions{
		BinaryPath: bin,
		Name:       "Crashy",
		Subdomain:  "crashy",
		Port:       freePort(t),
		Gateway:    "g",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool { return p.Status() == StatusStopped }, "process never exited")

	entries := loadState(t, path)
	if len(entries) != 1 || entries[0].Subdomain != "crashy" {
		t.Errorf("crashed named command should remain registered, got %+v", entries)
	}
}

func TestManager_ShutdownKeepsNamedRegistry(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\nsleep 30\n")
	path := filepath.Join(t.TempDir(), "state.json")
	m := New(10, 0, "")
	m.SetStore(NewFileStore(path, nil))

	for i, subdomain := range []string{"a", "b"} {
		if _, err := m.Start(StartOptions{
			BinaryPath: bin,
			Name:       "Command " + subdomain,
			Subdomain:  subdomain,
			Port:       freePort(t),
			Gateway:    "g",
		}); err != nil {
			t.Fatalf("start %d: %v", i, err)
		}
	}
	stopAll(t, m)
	if entries := loadState(t, path); len(entries) != 2 {
		t.Errorf("shutdown should preserve named registry, got %+v", entries)
	}
}

func TestManager_RestoreRegistersWithoutLaunching(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\nsleep 30\n")
	path := filepath.Join(t.TempDir(), "state.json")
	seed := NewFileStore(path, nil)
	if err := seed.Save([]StartOptions{
		{Name: "Good", BinaryPath: bin, Subdomain: "good", Port: freePort(t), Gateway: "g"},
		{Name: "Bad", BinaryPath: "/no/such/binary", Subdomain: "bad", Port: freePort(t), Gateway: "g"},
		{BinaryPath: bin, Subdomain: "adhoc", Port: freePort(t), Gateway: "g"},
	}); err != nil {
		t.Fatal(err)
	}

	m := New(10, 0, "")
	m.SetStore(seed)
	t.Cleanup(func() { stopAll(t, m) })

	results, err := m.Restore()
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("restore results = %+v, want two named registrations", results)
	}
	if _, ok := m.GetBySubdomain("good"); ok {
		t.Fatal("restore should register command without launching process")
	}
	if _, ok := m.GetDesired("good"); !ok {
		t.Fatal("restore did not register good command")
	}
	if _, ok := m.GetDesired("bad"); !ok {
		t.Fatal("restore did not register bad command")
	}
	if _, ok := m.GetDesired("adhoc"); ok {
		t.Fatal("restore should skip historical ad-hoc entries")
	}
	if entries := loadState(t, path); len(entries) != 3 {
		t.Errorf("restore should not rewrite state, got %+v", entries)
	}
}

func TestManager_RestartPreservesNamedRegistry(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\nsleep 30\n")
	path := filepath.Join(t.TempDir(), "state.json")
	m := New(10, 0, "")
	m.SetStore(NewFileStore(path, nil))
	t.Cleanup(func() { stopAll(t, m) })

	p, err := m.Start(StartOptions{
		BinaryPath: bin,
		Name:       "Restartable",
		Subdomain:  "rs",
		Port:       freePort(t),
		Gateway:    "g",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Restart(p.ID); err != nil {
		t.Fatal(err)
	}
	if entries := loadState(t, path); len(entries) != 1 || entries[0].Subdomain != "rs" {
		t.Errorf("restart should keep named command registered, got %+v", entries)
	}
}

func TestManager_UnregisterCommandStopsAndRemoves(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\nsleep 30\n")
	path := filepath.Join(t.TempDir(), "state.json")
	m := New(10, 0, "")
	m.SetStore(NewFileStore(path, nil))
	t.Cleanup(func() { stopAll(t, m) })

	p, err := m.Start(StartOptions{
		BinaryPath: bin,
		Name:       "Remove Me",
		Subdomain:  "remove-me",
		Port:       freePort(t),
		Gateway:    "g",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.UnregisterCommand("remove-me"); err != nil {
		t.Fatal(err)
	}
	if p.Status() != StatusStopped {
		t.Fatalf("process status = %s, want stopped", p.Status())
	}
	if _, ok := m.GetDesired("remove-me"); ok {
		t.Fatal("command still registered after unregister")
	}
	if entries := loadState(t, path); len(entries) != 0 {
		t.Fatalf("state after unregister = %+v, want empty", entries)
	}
	if err := m.UnregisterCommand("remove-me"); err == nil {
		t.Fatal("second unregister should fail")
	}
}

func TestManager_NoStoreIsNoop(t *testing.T) {
	bin := writeTestBin(t, "#!/bin/sh\nsleep 30\n")
	m := New(10, 0, "")
	t.Cleanup(func() { stopAll(t, m) })

	p, err := m.Start(StartOptions{BinaryPath: bin, Subdomain: "nostore", Port: freePort(t), Gateway: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Stop(p.ID); err != nil {
		t.Fatal(err)
	}
	results, err := m.Restore()
	if err != nil || results != nil {
		t.Errorf("Restore with no store = (%v, %v), want (nil, nil)", results, err)
	}
}

func loadState(t *testing.T, path string) []StartOptions {
	t.Helper()
	entries, err := NewFileStore(path, nil).Load()
	if err != nil {
		t.Fatalf("load state %s: %v", path, err)
	}
	return entries
}

package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// StateStore persists the named command registry. Manager writes the full
// registry on every change and reads it back once at boot.
type StateStore interface {
	Save(entries []StartOptions) error
	Load() ([]StartOptions, error)
}

// SetStore attaches a state store. Call it once, before Restore and before any
// Start/Stop traffic — there's no internal synchronisation around the field
// because the intended use is a single call during startup wiring.
func (m *Manager) SetStore(s StateStore) { m.store = s }

// remember adds opts to the named command registry and flushes the store.
// opts already has paths expanded/defaulted so it can be run later without
// resolving relative to a possibly different base dir.
func (m *Manager) remember(opts StartOptions) {
	m.mu.Lock()
	m.registry[opts.Subdomain] = opts
	snap := m.registrySnapshotLocked()
	m.mu.Unlock()
	m.persist(snap)
	m.broadcast(Event{Kind: EventKindRegistry, ID: opts.Subdomain})
}

// forget drops subdomain from the registry and flushes the result. A no-op
// and no write if subdomain was not tracked.
func (m *Manager) forget(subdomain string) {
	m.mu.Lock()
	if _, ok := m.registry[subdomain]; !ok {
		m.mu.Unlock()
		return
	}
	delete(m.registry, subdomain)
	snap := m.registrySnapshotLocked()
	m.mu.Unlock()
	m.persist(snap)
	m.broadcast(Event{Kind: EventKindRegistry, ID: subdomain})
}

// registrySnapshotLocked returns registry entries sorted by subdomain for
// deterministic on-disk output. Caller must hold m.mu.
func (m *Manager) registrySnapshotLocked() []StartOptions {
	subs := make([]string, 0, len(m.registry))
	for s := range m.registry {
		subs = append(subs, s)
	}
	sort.Strings(subs)
	out := make([]StartOptions, 0, len(subs))
	for _, s := range subs {
		out = append(out, m.registry[s])
	}
	return out
}

// persist is best-effort: a failed write is logged by the store, not bubbled
// up, because losing the ability to remember should never fail a live start or
// stop. The in-memory registry stays correct regardless.
func (m *Manager) persist(entries []StartOptions) {
	if m.store == nil {
		return
	}
	_ = m.store.Save(entries)
}

// RestoreResult records one persisted command loaded during restore.
type RestoreResult struct {
	Opts    StartOptions
	Process *Process // currently nil: restore registers commands without starting them
	Err     error
}

// Restore loads persisted named commands into the in-memory registry without
// launching them. A nil store returns (nil, nil). The returned error only
// reports load failure; historical unnamed entries are ignored.
func (m *Manager) Restore() ([]RestoreResult, error) {
	if m.store == nil {
		return nil, nil
	}
	entries, err := m.store.Load()
	if err != nil {
		return nil, err
	}

	results := make([]RestoreResult, 0, len(entries))
	m.mu.Lock()
	for _, e := range entries {
		if e.Name == "" {
			// Historical state files may contain ad-hoc processes from earlier
			// builds. Keep ad-hoc processes ephemeral by not importing them.
			continue
		}
		m.registry[e.Subdomain] = e
		results = append(results, RestoreResult{Opts: e})
	}
	m.mu.Unlock()
	return results, nil
}

// FileStore persists registry entries as a JSON array in a single file. Writes
// are atomic (temp file + rename) so a crash mid-write cannot leave a
// truncated state file that fails to parse on next boot.
type FileStore struct {
	path   string
	logger *slog.Logger
	mu     sync.Mutex
}

func NewFileStore(path string, logger *slog.Logger) *FileStore {
	if logger == nil {
		logger = slog.Default()
	}
	return &FileStore{path: path, logger: logger}
}

func (s *FileStore) Save(entries []StartOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		s.logger.Warn("state dir create failed", "path", s.path, "err", err.Error())
		return err
	}
	b, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		s.logger.Warn("state write failed", "path", s.path, "err", err.Error())
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		s.logger.Warn("state rename failed", "path", s.path, "err", err.Error())
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func (s *FileStore) Load() ([]StartOptions, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil // first boot: nothing to restore
	}
	if err != nil {
		return nil, err
	}
	var entries []StartOptions
	if err := json.Unmarshal(b, &entries); err != nil {
		return nil, fmt.Errorf("parse state file %s: %w", s.path, err)
	}
	return entries, nil
}

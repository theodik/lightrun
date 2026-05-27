package manager

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Status int

const (
	StatusRunning Status = iota
	StatusStopped
)

func (s Status) String() string {
	switch s {
	case StatusRunning:
		return "running"
	case StatusStopped:
		return "stopped"
	}
	return "unknown"
}

// Health reflects whether the declared port has been observed to accept TCP
// connections. It's orthogonal to Status: a stopped process keeps whatever
// health value it last had (e.g. "starting" if it exited before the probe
// reached a verdict). The two fields together let callers distinguish "running
// and serving" from "running but bound to a different port / not bound yet".
type Health int

const (
	HealthStarting  Health = iota // process up, probe still in its window
	HealthHealthy                 // probe successfully connected to declared port
	HealthUnhealthy               // probe window elapsed without a successful connect
)

func (h Health) String() string {
	switch h {
	case HealthStarting:
		return "starting"
	case HealthHealthy:
		return "healthy"
	case HealthUnhealthy:
		return "unhealthy"
	}
	return "unknown"
}

// Default probe timings; copied into each Manager at New(). Per-Manager fields
// avoid cross-test races when tests want a shorter window. Don't mutate these
// — set the corresponding Manager field on a freshly constructed instance.
const (
	defaultProbeTimeout  = 5 * time.Second
	defaultProbeInterval = 200 * time.Millisecond
)

type Process struct {
	ID         string
	BinaryPath string
	WorkingDir string
	Subdomain  string
	Port       int
	Gateway    string
	StartTime  time.Time

	mu       sync.RWMutex
	cmd      *exec.Cmd
	status   Status
	health   Health
	stopTime time.Time // set by reap once status flips to Stopped; used by janitor TTL
	exitErr  error
	exitCode int
	logs     *ringBuffer
	done     chan struct{}
	opts     StartOptions // remembered for Restart
}

func (p *Process) Health() Health {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.health
}

func (p *Process) Status() Status {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.status
}

func (p *Process) ExitInfo() (int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.exitCode, p.exitErr
}

func (p *Process) Tail(n int) []string {
	return p.logs.tail(n)
}

type StartOptions struct {
	BinaryPath string
	Subdomain  string
	Port       int
	Env        map[string]string
	Gateway    string
	// WorkingDir is the directory the child process is launched in. Empty
	// means "use the directory containing BinaryPath" — most apps expect to
	// run from next to their own assets (web/, tmp/, ...), so that default
	// matches typical run.sh behaviour.
	WorkingDir string
}

type Manager struct {
	mu          sync.RWMutex
	procs       map[string]*Process
	bySubdomain map[string]*Process
	byPort      map[int]*Process

	logSize       int
	stoppedTTL    time.Duration
	probeTimeout  time.Duration
	probeInterval time.Duration
	binaryBaseDir string // empty disables '~' expansion
}

// janitorInterval is the period at which the background sweeper checks for
// stopped processes older than stoppedTTL. Package-level so tests can shorten
// it; do NOT mutate from production code.
var janitorInterval = 1 * time.Minute

// New creates a Manager and, if stoppedTTL > 0, spawns a background janitor
// that drops stopped Process entries older than stoppedTTL. Set stoppedTTL = 0
// to keep stopped entries forever (useful in tests / short-lived runs).
// binaryBaseDir is the directory used to resolve a leading '~' (or '~/...') in
// StartOptions.BinaryPath; pass "" to disable that expansion.
func New(logSize int, stoppedTTL time.Duration, binaryBaseDir string) *Manager {
	m := &Manager{
		procs:         make(map[string]*Process),
		bySubdomain:   make(map[string]*Process),
		byPort:        make(map[int]*Process),
		logSize:       logSize,
		stoppedTTL:    stoppedTTL,
		probeTimeout:  defaultProbeTimeout,
		probeInterval: defaultProbeInterval,
		binaryBaseDir: binaryBaseDir,
	}
	if stoppedTTL > 0 {
		go m.runJanitor()
	}
	return m
}

func (m *Manager) runJanitor() {
	t := time.NewTicker(janitorInterval)
	defer t.Stop()
	for range t.C {
		m.sweepStopped()
	}
}

func (m *Manager) sweepStopped() {
	if m.stoppedTTL <= 0 {
		return
	}
	cutoff := time.Now().Add(-m.stoppedTTL)
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, p := range m.procs {
		p.mu.RLock()
		drop := p.status == StatusStopped && !p.stopTime.IsZero() && p.stopTime.Before(cutoff)
		p.mu.RUnlock()
		if drop {
			delete(m.procs, id)
		}
	}
}

var subdomainRe = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

// childEnvDenyPrefixes lists env-var prefixes stripped from the inherited
// environment when spawning a child. The goal is "lightrun is invisible to the
// app": pass through whatever the container has, minus lightrun's own config.
var childEnvDenyPrefixes = []string{"LIGHTRUN_"}

func (m *Manager) Start(opts StartOptions) (*Process, error) {
	if !subdomainRe.MatchString(opts.Subdomain) {
		return nil, fmt.Errorf("invalid subdomain %q", opts.Subdomain)
	}
	if opts.Port <= 0 || opts.Port > 65535 {
		return nil, fmt.Errorf("invalid port %d", opts.Port)
	}
	opts.BinaryPath = expandBinaryPath(opts.BinaryPath, m.binaryBaseDir)
	if err := checkBinary(opts.BinaryPath); err != nil {
		return nil, err
	}
	if opts.WorkingDir == "" {
		opts.WorkingDir = filepath.Dir(opts.BinaryPath)
	} else {
		opts.WorkingDir = expandBinaryPath(opts.WorkingDir, m.binaryBaseDir)
		if err := checkWorkingDir(opts.WorkingDir); err != nil {
			return nil, err
		}
	}

	m.mu.Lock()
	if existing, ok := m.bySubdomain[opts.Subdomain]; ok && existing.Status() == StatusRunning {
		m.mu.Unlock()
		return nil, fmt.Errorf("subdomain %q already in use by process %s", opts.Subdomain, existing.ID)
	}
	if existing, ok := m.byPort[opts.Port]; ok && existing.Status() == StatusRunning {
		m.mu.Unlock()
		return nil, fmt.Errorf("port %d already in use by process %s (subdomain %q)", opts.Port, existing.ID, existing.Subdomain)
	}
	if !portFree(opts.Port) {
		m.mu.Unlock()
		return nil, fmt.Errorf("port %d is already bound by another process on the host", opts.Port)
	}
	id, err := m.newIDLocked()
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}

	p := &Process{
		ID:         id,
		BinaryPath: opts.BinaryPath,
		WorkingDir: opts.WorkingDir,
		Subdomain:  opts.Subdomain,
		Port:       opts.Port,
		Gateway:    opts.Gateway,
		StartTime:  time.Now(),
		status:     StatusRunning,
		logs:       newRingBuffer(m.logSize),
		done:       make(chan struct{}),
		opts:       opts,
	}

	cmd := exec.Command(opts.BinaryPath)
	cmd.Dir = opts.WorkingDir
	cmd.Env = buildChildEnv(opts.Port, opts.Env)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid:   true,
		Pdeathsig: syscall.SIGTERM,
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		m.mu.Unlock()
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		m.mu.Unlock()
		return nil, fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		m.mu.Unlock()
		return nil, classifyExecError(err, opts.BinaryPath)
	}
	p.cmd = cmd

	m.procs[id] = p
	m.bySubdomain[opts.Subdomain] = p
	m.byPort[opts.Port] = p
	m.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go pipeIntoBuffer(stdout, p.logs, &wg)
	go pipeIntoBuffer(stderr, p.logs, &wg)

	go m.reap(p, &wg)
	go m.probeHealth(p)

	return p, nil
}

// probeHealth periodically TCP-dials the declared port until either a connect
// succeeds (→ healthy), the process exits (probe stops, health left as-is for
// the caller to interpret alongside status=stopped), or the timeout window
// elapses (→ unhealthy). Runs in its own goroutine; cheap and best-effort.
func (m *Manager) probeHealth(p *Process) {
	// "localhost" so Go's resolver returns both 127.0.0.1 and ::1 — Dial then
	// tries each, so an app bound only on IPv6 still reads as healthy. The
	// startup portFree check still binds IPv4 only, which is fine: the kernel
	// rejects overlapping binds across families for any app using the typical
	// 0.0.0.0 / [::] / dual-stack wildcards.
	addr := net.JoinHostPort("localhost", strconv.Itoa(p.Port))
	ticker := time.NewTicker(m.probeInterval)
	defer ticker.Stop()
	deadline := time.After(m.probeTimeout)

	tryConnect := func() bool {
		conn, err := net.DialTimeout("tcp", addr, m.probeInterval)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}

	if tryConnect() {
		p.mu.Lock()
		p.health = HealthHealthy
		p.mu.Unlock()
		return
	}

	for {
		select {
		case <-p.done:
			return
		case <-deadline:
			p.mu.Lock()
			if p.status == StatusRunning {
				p.health = HealthUnhealthy
			}
			p.mu.Unlock()
			return
		case <-ticker.C:
			if tryConnect() {
				p.mu.Lock()
				p.health = HealthHealthy
				p.mu.Unlock()
				return
			}
		}
	}
}

func (m *Manager) reap(p *Process, wg *sync.WaitGroup) {
	wg.Wait()
	err := p.cmd.Wait()

	p.mu.Lock()
	p.status = StatusStopped
	p.stopTime = time.Now()
	p.exitErr = err
	if p.cmd.ProcessState != nil {
		p.exitCode = p.cmd.ProcessState.ExitCode()
	}
	p.mu.Unlock()
	close(p.done)

	m.mu.Lock()
	if cur, ok := m.bySubdomain[p.Subdomain]; ok && cur == p {
		delete(m.bySubdomain, p.Subdomain)
	}
	if cur, ok := m.byPort[p.Port]; ok && cur == p {
		delete(m.byPort, p.Port)
	}
	m.mu.Unlock()
}

func (m *Manager) Stop(id string) error {
	m.mu.RLock()
	p, ok := m.procs[id]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("process %q not found", id)
	}
	if p.Status() == StatusStopped {
		return nil
	}

	pgid, err := syscall.Getpgid(p.cmd.Process.Pid)
	if err != nil {
		pgid = p.cmd.Process.Pid
	}
	_ = syscall.Kill(-pgid, syscall.SIGTERM)

	select {
	case <-p.done:
		return nil
	case <-time.After(5 * time.Second):
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
	}

	select {
	case <-p.done:
	case <-time.After(2 * time.Second):
		return errors.New("process did not exit after SIGKILL")
	}
	return nil
}

// Restart stops the process with the given id (if running) and starts a new
// one with the same StartOptions. The old Process entry stays in the map with
// status=stopped so its logs and exit info remain accessible by the old id.
// Returns the new Process (with a fresh id).
func (m *Manager) Restart(id string) (*Process, error) {
	m.mu.RLock()
	old, ok := m.procs[id]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("process %q not found", id)
	}
	if old.Status() == StatusRunning {
		if err := m.Stop(id); err != nil {
			return nil, fmt.Errorf("stop before restart: %w", err)
		}
	}
	return m.Start(old.opts)
}

func (m *Manager) Get(id string) (*Process, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.procs[id]
	return p, ok
}

func (m *Manager) GetBySubdomain(subdomain string) (*Process, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.bySubdomain[subdomain]
	return p, ok
}

func (m *Manager) List() []*Process {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Process, 0, len(m.procs))
	for _, p := range m.procs {
		out = append(out, p)
	}
	return out
}

func (m *Manager) StopAll(ctx context.Context) {
	m.mu.RLock()
	ids := make([]string, 0, len(m.procs))
	for id, p := range m.procs {
		if p.Status() == StatusRunning {
			ids = append(ids, id)
		}
	}
	m.mu.RUnlock()

	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_ = m.Stop(id)
		}(id)
	}

	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-ctx.Done():
	}
}

// checkBinary distinguishes "not found", "is a directory", "permission denied
// on stat" and "no executable bit set" so the start tool can tell the caller
// *why* their binary was rejected without a follow-up exec attempt. Exec-time
// failures (wrong arch, missing shebang interpreter, noexec mount) are handled
// separately by classifyExecError after cmd.Start.
func checkBinary(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist):
			return fmt.Errorf("binary not found: %s", path)
		case errors.Is(err, os.ErrPermission):
			return fmt.Errorf("permission denied reading binary %s (lightrun uid=%d)", path, os.Getuid())
		}
		return fmt.Errorf("stat binary %s: %w", path, err)
	}
	if info.IsDir() {
		return fmt.Errorf("binary path is a directory: %s", path)
	}
	if info.Mode()&0o111 == 0 {
		return fmt.Errorf("binary is not executable: no executable bit on %s (mode %v)", path, info.Mode().Perm())
	}
	return nil
}

// checkWorkingDir mirrors checkBinary's error granularity for an explicit
// working_dir override: callers want to know whether their path was missing,
// inaccessible, or just a regular file before they get a generic exec failure.
func checkWorkingDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		switch {
		case errors.Is(err, os.ErrNotExist):
			return fmt.Errorf("working_dir not found: %s", path)
		case errors.Is(err, os.ErrPermission):
			return fmt.Errorf("permission denied reading working_dir %s (lightrun uid=%d)", path, os.Getuid())
		}
		return fmt.Errorf("stat working_dir %s: %w", path, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("working_dir is not a directory: %s", path)
	}
	return nil
}

// classifyExecError unwraps the os.PathError that exec.Cmd.Start returns when
// the kernel rejects the exec, mapping the underlying errno to a message that
// names a likely cause. Falls back to a generic wrap when the errno isn't one
// we recognise.
func classifyExecError(err error, path string) error {
	var perr *os.PathError
	if errors.As(err, &perr) {
		switch {
		case errors.Is(perr.Err, syscall.ENOENT):
			return fmt.Errorf("exec failed for %s: file or shebang interpreter not found", path)
		case errors.Is(perr.Err, syscall.ENOEXEC):
			return fmt.Errorf("exec failed for %s: bad binary format (wrong architecture or not an executable)", path)
		case errors.Is(perr.Err, syscall.EACCES):
			return fmt.Errorf("exec failed for %s: permission denied (filesystem may be mounted noexec, or file lacks exec for uid=%d)", path, os.Getuid())
		}
	}
	return fmt.Errorf("start %s: %w", path, err)
}

// expandBinaryPath resolves a leading '~' (alone) or '~/...' against base.
// base == "" disables expansion (path returned verbatim). '~user/...' style
// per-user lookups are not supported and are returned unchanged.
func expandBinaryPath(path, base string) string {
	if base == "" || path == "" || path[0] != '~' {
		return path
	}
	if path == "~" {
		return base
	}
	if strings.HasPrefix(path, "~/") {
		return filepath.Join(base, path[2:])
	}
	return path
}

func portFree(port int) bool {
	l, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
	if err != nil {
		return false
	}
	_ = l.Close()
	return true
}

func (m *Manager) newIDLocked() (string, error) {
	for range 32 {
		var b [2]byte
		if _, err := rand.Read(b[:]); err != nil {
			return "", err
		}
		id := hex.EncodeToString(b[:])
		if _, exists := m.procs[id]; !exists {
			return id, nil
		}
	}
	return "", errors.New("could not allocate unique id")
}

func buildChildEnv(port int, extra map[string]string) []string {
	parent := os.Environ()
	env := make([]string, 0, len(parent)+1+len(extra))
	for _, kv := range parent {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		k := kv[:eq]
		if k == "PORT" || hasAnyPrefix(k, childEnvDenyPrefixes) {
			continue
		}
		env = append(env, kv)
	}
	env = append(env, "PORT="+strconv.Itoa(port))
	for k, v := range extra {
		env = append(env, k+"="+v)
	}
	return env
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

func pipeIntoBuffer(r io.Reader, buf *ringBuffer, wg *sync.WaitGroup) {
	defer wg.Done()
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		buf.add(scanner.Text())
	}
}

// ringBuffer is a fixed-size circular line buffer.
type ringBuffer struct {
	mu    sync.RWMutex
	lines []string
	head  int
	size  int
	cap   int
}

func newRingBuffer(capacity int) *ringBuffer {
	if capacity <= 0 {
		capacity = 1
	}
	return &ringBuffer{lines: make([]string, capacity), cap: capacity}
}

func (b *ringBuffer) add(line string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lines[b.head] = line
	b.head = (b.head + 1) % b.cap
	if b.size < b.cap {
		b.size++
	}
}

func (b *ringBuffer) tail(n int) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if n <= 0 || n > b.size {
		n = b.size
	}
	out := make([]string, n)
	start := (b.head - n + b.cap) % b.cap
	for i := 0; i < n; i++ {
		out[i] = b.lines[(start+i)%b.cap]
	}
	return out
}

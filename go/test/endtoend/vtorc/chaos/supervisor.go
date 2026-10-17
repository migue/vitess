/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package chaos

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

// restartPolicy is how a supervisor delays restarting a vttablet that exited by itself
// (local, #21549 A/B experiment). Restarts the harness causes are never delayed.
type restartPolicy struct {
	Name string
	// First is the delay after the first exit; each further exit doubles it, up to Max.
	First, Max time.Duration
	// Reset forgets earlier exits once a generation has run this long.
	Reset time.Duration
}

var restartPolicies = map[string]restartPolicy{
	// Kubernetes CrashLoopBackOff.
	"k8s": {Name: "k8s", First: 10 * time.Second, Max: 5 * time.Minute, Reset: 10 * time.Minute},
	// systemd Restart=always, RestartSec=1.
	"fast": {Name: "fast", First: time.Second, Max: time.Second, Reset: 10 * time.Minute},
}

// selectedRestartPolicy reads CHAOS_SUPERVISOR (k8s or fast; default fast).
func selectedRestartPolicy() (restartPolicy, error) {
	name := os.Getenv("CHAOS_SUPERVISOR")
	if name == "" {
		name = "fast"
	}
	p, ok := restartPolicies[name]
	if !ok {
		return restartPolicy{}, fmt.Errorf("unknown CHAOS_SUPERVISOR %q (k8s or fast)", name)
	}
	return p, nil
}

func (p restartPolicy) delay(exits int) time.Duration {
	d := p.First
	for i := 1; i < exits && d < p.Max; i++ {
		d *= 2
	}
	return min(d, p.Max)
}

// Generation is one process of a supervised vttablet.
type Generation struct {
	N     int
	Start time.Time
	End   time.Time `json:",omitzero"`
	// Induced: the harness killed it (only generation 0, when supervision starts). Every later
	// generation that ended before the scenario stopped the supervisor exited by itself.
	Induced bool
	// Teardown: the harness killed it when the scenario stopped the supervisor; not an exit.
	Teardown bool   `json:",omitempty"`
	ExitErr  string `json:",omitempty"`
	LastLine string `json:",omitempty"`
	// Delay is how long the supervisor waited before starting this generation.
	Delay   time.Duration
	Log     string
	Argv    []string `json:",omitempty"`
	ArgvSHA string   `json:",omitempty"`
	EnvSHA  string   `json:",omitempty"`
}

// Supervisor runs a node's vttablet the way a process supervisor would: when it exits it is
// started again with the same arguments and environment after the policy's delay. Each
// generation has its own exec.Cmd, exit channel and log file; VttabletProcess shares one exit
// field across Setup calls, so after a quick restart an older generation's waiter could see the
// newer process.
type Supervisor struct {
	s       *Scenario
	n       *Node
	policy  restartPolicy
	restore bool
	argv    []string
	env     []string

	mu      sync.Mutex
	gens    []*Generation
	cmd     *exec.Cmd
	stopped bool
	stop    chan struct{}
	done    chan struct{}
}

// Supervise kills the node's vttablet (generation 0, started by the cluster) and from then on
// runs it under a supervisor. restore keeps --restore-from-backup on every generation. prepare,
// if set, runs between the kill and the first supervised start.
func (s *Scenario) Supervise(n *Node, restore bool, prepare func()) *Supervisor {
	policy, err := selectedRestartPolicy()
	if err != nil {
		s.t.Fatal(err)
	}
	vp := n.Tablet.VttabletProcess
	sv := &Supervisor{s: s, n: n, policy: policy, restore: restore, stop: make(chan struct{}), done: make(chan struct{})}
	sv.argv = vttabletArgv(vp, restore)
	sv.env = append(os.Environ(), cluster.DefaultVttestEnv)

	g0 := &Generation{N: 0, Log: strings.TrimSuffix(vp.ErrorLog, ".txt") + "-gen0.txt", Induced: true}
	if vp.ErrorLog != "" {
		b, _ := os.ReadFile(vp.ErrorLog)
		_ = os.WriteFile(g0.Log, b, 0o644)
	}
	_ = vp.Kill()
	g0.End = time.Now()
	s.Log.Add("fault", fmt.Sprintf("kill -9 vttablet of %s (generation 0); supervised from now on, policy %s, --restore-from-backup=%v", n.Tablet.Alias, policy.Name, restore))
	sv.gens = append(sv.gens, g0)
	if prepare != nil {
		prepare()
	}
	s.t.Cleanup(sv.Stop)
	go sv.run()
	return sv
}

// vttabletArgv is the command line VttabletProcess.Setup builds, without the coverage and
// pprof flags the harness never sets.
func vttabletArgv(vp *cluster.VttabletProcess, restore bool) []string {
	argv := []string{
		vp.Binary,
		"--topo-implementation", vp.TopoImplementation,
		"--topo-global-server-address", vp.TopoGlobalAddress,
		"--topo-global-root", vp.TopoGlobalRoot,
		"--log-queries-to-file", vp.FileToLogQueries,
		"--tablet-path", vp.TabletPath,
		"--port", strconv.Itoa(vp.Port),
		"--grpc-port", strconv.Itoa(vp.GrpcPort),
		"--init-shard", vp.Shard,
		"--tablet-hostname", vp.TabletHostname,
		"--init-keyspace", vp.Keyspace,
		"--init-tablet-type", vp.TabletType,
		"--health-check-interval", fmt.Sprintf("%ds", vp.HealthCheckInterval),
		"--enable-replication-reporter",
		"--backup-storage-implementation", vp.BackupStorageImplementation,
		"--file-backup-storage-root", vp.FileBackupStorageRoot,
		"--service-map", vp.ServiceMap,
		"--db-charset", vp.Charset,
		"--bind-address", "127.0.0.1",
		"--grpc-bind-address", "127.0.0.1",
		"--log-format", "text",
	}
	if restore {
		argv = append(argv, "--restore-from-backup")
	}
	if vp.DbFlavor != "" {
		argv = append(argv, "--db-flavor="+vp.DbFlavor)
	}
	return append(argv, vp.ExtraArgs...)
}

func shaOf(l []string) string {
	h := sha256.Sum256([]byte(strings.Join(l, "\x00")))
	return hex.EncodeToString(h[:8])
}

func (sv *Supervisor) run() {
	defer close(sv.done)
	var delay time.Duration
	exits := 0
	for n := 1; ; n++ {
		if delay > 0 {
			select {
			case <-sv.stop:
				return
			case <-time.After(delay):
			}
		}
		g, exited, err := sv.start(n, delay)
		if err != nil {
			sv.s.Log.Add("supervisor", fmt.Sprintf("%s generation %d did not start: %v", sv.n.Tablet.Alias, n, err))
			return
		}
		exitErr := <-exited
		end, lastLine := time.Now(), lastErrorLine(g.Log)
		sv.mu.Lock()
		g.End, g.LastLine = end, lastLine
		if exitErr != nil {
			g.ExitErr = exitErr.Error()
		}
		stopped := sv.stopped
		g.Teardown = stopped
		sv.mu.Unlock()
		if stopped {
			return
		}
		if g.End.Sub(g.Start) >= sv.policy.Reset {
			exits = 0
		}
		exits++
		delay = sv.policy.delay(exits)
		sv.s.Log.Add("supervisor", fmt.Sprintf("%s generation %d exited by itself after %.1fs (%v): %s; restarting in %v",
			sv.n.Tablet.Alias, n, g.End.Sub(g.Start).Seconds(), exitErr, g.LastLine, delay))
	}
}

// start starts generation n and returns it with a channel that receives its exit.
func (sv *Supervisor) start(n int, delay time.Duration) (*Generation, <-chan error, error) {
	vp := sv.n.Tablet.VttabletProcess
	g := &Generation{
		N: n, Delay: delay, Argv: sv.argv, ArgvSHA: shaOf(sv.argv), EnvSHA: shaOf(sv.env),
		Log: path.Join(vp.LogDir, fmt.Sprintf("%s-vttablet-stderr-gen%d.txt", vp.TabletPath, n)),
	}
	logFile, err := os.Create(g.Log)
	if err != nil {
		return nil, nil, err
	}
	cmd := exec.Command(sv.argv[0], sv.argv[1:]...)
	cmd.Env = sv.env
	cmd.Stderr = logFile
	sv.mu.Lock()
	defer sv.mu.Unlock()
	if sv.stopped {
		logFile.Close()
		return nil, nil, fmt.Errorf("supervisor stopped")
	}
	g.Start = time.Now()
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, nil, err
	}
	sv.cmd = cmd
	sv.gens = append(sv.gens, g)
	// Scenario helpers that grep "the tablet's log" read the current generation's.
	vp.ErrorLog = g.Log
	exited := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		logFile.Close()
		exited <- err
	}()
	sv.s.Log.Add("supervisor", fmt.Sprintf("%s generation %d started (pid %d, after %v)", sv.n.Tablet.Alias, n, cmd.Process.Pid, delay))
	return g, exited, nil
}

// Running reports whether the current generation is running, and its number.
func (sv *Supervisor) Running() (int, bool) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	g := sv.gens[len(sv.gens)-1]
	return g.N, g.N > 0 && g.End.IsZero()
}

// Generations returns a copy of the generations so far.
func (sv *Supervisor) Generations() []Generation {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	var r []Generation
	for _, g := range sv.gens {
		r = append(r, *g)
	}
	return r
}

// Stop ends supervision, kills the running generation, and saves the generation records.
func (sv *Supervisor) Stop() {
	sv.mu.Lock()
	if sv.stopped {
		sv.mu.Unlock()
		return
	}
	sv.stopped = true
	close(sv.stop)
	if sv.cmd != nil {
		_ = sv.cmd.Process.Kill()
	}
	sv.mu.Unlock()
	select {
	case <-sv.done:
	case <-time.After(30 * time.Second):
		sv.s.Log.Add("supervisor", sv.n.Tablet.Alias+": supervisor did not stop within 30s")
	}
	sv.save()
}

// Check reports harness violations: generations whose command line or environment differ.
func (sv *Supervisor) Check() {
	gens := sv.Generations()
	for _, g := range gens[1:] {
		if g.ArgvSHA != gens[1].ArgvSHA || g.EnvSHA != gens[1].EnvSHA {
			sv.s.R.violation("HARNESS: %s generation %d argv/env %s/%s differ from generation 1's %s/%s",
				sv.n.Tablet.Alias, g.N, g.ArgvSHA, g.EnvSHA, gens[1].ArgvSHA, gens[1].EnvSHA)
		}
	}
}

func (sv *Supervisor) save() {
	d := path.Join(resultsDir(), sv.s.R.Name)
	_ = os.MkdirAll(d, 0o755)
	gens := sv.Generations()
	b, _ := json.MarshalIndent(struct {
		Tablet      string
		Policy      restartPolicy
		Restore     bool
		Generations []Generation
	}{sv.n.Tablet.Alias, sv.policy, sv.restore, gens}, "", "  ")
	_ = os.WriteFile(path.Join(d, "supervisor-"+sv.n.Tablet.Alias+".json"), b, 0o644)
	for _, g := range gens {
		if g.N > 0 {
			_ = exec.Command("cp", g.Log, path.Join(d, path.Base(g.Log))).Run()
		}
	}
}

// Summary describes the generations for the report.
func (sv *Supervisor) Summary() string {
	var parts []string
	for _, g := range sv.Generations() {
		if g.N == 0 {
			continue
		}
		end := "running"
		if !g.End.IsZero() {
			kind := "exited by itself"
			if g.Induced {
				kind = "induced"
			}
			end = fmt.Sprintf("%s after %.1fs (%s)", kind, g.End.Sub(g.Start).Seconds(), g.LastLine)
		}
		parts = append(parts, fmt.Sprintf("gen%d start %s delay %v: %s", g.N, g.Start.Format("15:04:05.000"), g.Delay, end))
	}
	return strings.Join(parts, " || ")
}

// SelfExits counts the generations that exited by themselves.
func (sv *Supervisor) SelfExits() int {
	return len(slices.DeleteFunc(sv.Generations(), func(g Generation) bool { return g.N == 0 || g.End.IsZero() || g.Induced || g.Teardown }))
}

// lastErrorLine returns the last error-level line of a Vitess log, shortened.
func lastErrorLine(file string) string {
	b, err := os.ReadFile(file)
	if err != nil {
		return ""
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		l := lines[i]
		if strings.Contains(l, " ERR ") || strings.Contains(l, " FTL ") || (len(l) > 1 && (l[0] == 'E' || l[0] == 'F') && l[1] >= '0' && l[1] <= '9') {
			return l[:min(len(l), 300)]
		}
	}
	if len(lines) > 0 {
		l := lines[len(lines)-1]
		return l[:min(len(l), 300)]
	}
	return ""
}

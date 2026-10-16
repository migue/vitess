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
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// The A/B experiment for #21549 (local): does a starting vttablet that exits while the shard
// lock is held (design B) slow down or stop a reparent's convergence, compared with one that
// defers its replication initialization (design A)? See t2-pr21549-b-experiment-plan.md.
//
// Layout (E1, E1-vtorc, E2, E4): P = zone1-100 (primary), R1 = zone2-200 and R2 = zone3-300 (the
// restarted replicas), R3 = zone2-400 (the reparent target), R4 = zone3-500 (an acker for R3, so
// that the reparent stays safe without R1 and R2). E3 uses the three-tablet layout.
//
// The supervisor policy comes from CHAOS_SUPERVISOR (k8s or fast).

// abObservationWindow is how long after the first reparent attempt a run is observed. A run that
// has not converged by then is censored.
const abObservationWindow = 10 * time.Minute

// ---- replication poller ----

type replSample struct {
	T      time.Time
	Node   string
	Port   string // Source_Port; empty without replica status
	IO     string
	SQL    string
	Status string // vttablet TabletStateName; empty when unreachable
}

type replPoller struct {
	s       *Scenario
	mu      sync.Mutex
	samples []replSample
	stop    chan struct{}
	done    chan struct{}
}

func (s *Scenario) startReplPoller() *replPoller {
	rp := &replPoller{s: s, stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(rp.done)
		for {
			select {
			case <-rp.stop:
				return
			case <-time.After(250 * time.Millisecond):
			}
			var wg sync.WaitGroup
			for _, n := range s.Nodes {
				wg.Go(func() {
					smp := replSample{Node: n.Tablet.Alias, Status: tabletState(n)}
					if rs, err := n.replicaStatus(); err == nil && rs != nil {
						smp.Port, smp.IO, smp.SQL = rs["Source_Port"], rs["Replica_IO_Running"], rs["Replica_SQL_Running"]
					}
					smp.T = time.Now()
					rp.mu.Lock()
					rp.samples = append(rp.samples, smp)
					rp.mu.Unlock()
				})
			}
			wg.Wait()
		}
	}()
	s.t.Cleanup(rp.Stop)
	return rp
}

// Stop stops sampling and saves the samples.
func (rp *replPoller) Stop() {
	select {
	case <-rp.stop:
		return
	default:
	}
	close(rp.stop)
	<-rp.done
	d := path.Join(resultsDir(), rp.s.R.Name)
	_ = os.MkdirAll(d, 0o755)
	var b strings.Builder
	b.WriteString("time\ttablet\tsource_port\tio\tsql\tstate\n")
	for _, smp := range rp.Samples() {
		fmt.Fprintf(&b, "%s\t%s\t%s\t%s\t%s\t%s\n", smp.T.Format("15:04:05.000"), smp.Node, smp.Port, smp.IO, smp.SQL, smp.Status)
	}
	_ = os.WriteFile(path.Join(d, "repl-samples.tsv"), []byte(b.String()), 0o644)
}

func (rp *replPoller) Samples() []replSample {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	r := slices.Clone(rp.samples)
	sort.SliceStable(r, func(i, j int) bool { return r[i].T.Before(r[j].T) })
	return r
}

// tabletState returns the vttablet's TabletStateName, or "" if it does not answer.
func tabletState(n *Node) string {
	vp := n.Tablet.VttabletProcess
	resp, err := httpClient.Get(fmt.Sprintf("http://%s:%d/debug/vars", vp.TabletHostname, vp.Port))
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var v struct{ TabletStateName string }
	if json.NewDecoder(resp.Body).Decode(&v) != nil {
		return ""
	}
	return v.TabletStateName
}

// ---- log timelines ----

var ansiRe = regexp.MustCompile(`\x1b\[[0-9;]*m`)

// logTime parses the timestamp that starts a Vitess text log line. Vitess logs in the host's
// local time zone, which the harness shares.
func logTime(line string) (time.Time, bool) {
	line = ansiRe.ReplaceAllString(line, "")
	if len(line) < 23 {
		return time.Time{}, false
	}
	t, err := time.ParseInLocation("2006-01-02 15:04:05.000", line[:23], time.Local)
	return t, err == nil
}

// logMatches returns the time and text of the lines of the files that match re and are stamped
// at or after since.
func logMatches(files []string, re *regexp.Regexp, since time.Time) []logLine {
	var r []logLine
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(fh)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			l := sc.Text()
			if !re.MatchString(l) {
				continue
			}
			if t, ok := logTime(l); ok && !t.Before(since) {
				r = append(r, logLine{T: t, File: filepath.Base(f), Text: ansiRe.ReplaceAllString(l, "")})
			}
		}
		fh.Close()
	}
	sort.SliceStable(r, func(i, j int) bool { return r[i].T.Before(r[j].T) })
	return r
}

type logLine struct {
	T    time.Time
	File string
	Text string
}

// reparentAttempt is one shard lock hold by a reparent, from its log.
type reparentAttempt struct {
	Who    string
	Action string
	Lock   time.Time // the "Locking shard" line (logged before the acquisition)
	Unlock time.Time `json:",omitzero"` // zero while it holds the lock
	OK     bool
	Err    string `json:",omitempty"`
}

var (
	ersAction   = `EmergencyReparentShard[^ ]*`
	prsAction   = `PlannedReparentShard\([^)]*\)`
	vtorcAction = `VTOrc Recovery for (DeadPrimary|PrimarySemiSyncBlocked|PrimaryTabletUnreachableByQuorum|IncapacitatedPrimary|DeadPrimaryAndSomeReplicas|PrimaryDiskStalled) on [a-z0-9-]+`
	lockRe      = regexp.MustCompile(`Locking shard ` + keyspaceName + `/` + shardName + ` for action (.+) with options`)
	unlockRe    = regexp.MustCompile(`Unlocking shard ` + keyspaceName + `/` + shardName + ` for (successful )?action (.+?)(?: with error (.*))?$`)
)

// reparentAttempts pairs the lock and unlock lines of the actions matching actionRe in the
// vtctld and VTOrc logs since the given time.
func (s *Scenario) reparentAttempts(actionRe string, since time.Time) []reparentAttempt {
	files, _ := filepath.Glob(path.Join(s.CI.TmpDirectory, "vtorc-*-stderr.txt"))
	files = append(files, path.Join(s.CI.TmpDirectory, "vtctld-stderr.txt"))
	return pairLockLines(logMatches(files, regexp.MustCompile(`(Locking|Unlocking) shard `+keyspaceName+"/"+shardName), since), actionRe)
}

// pairLockLines pairs each log file's lock and unlock lines of the actions matching actionRe.
func pairLockLines(lines []logLine, actionRe string) []reparentAttempt {
	are := regexp.MustCompile(`^(` + actionRe + `)$`)
	open := map[string]*reparentAttempt{}
	var res []*reparentAttempt
	for _, l := range lines {
		if m := lockRe.FindStringSubmatch(l.Text); m != nil && are.MatchString(m[1]) {
			a := &reparentAttempt{Who: strings.TrimSuffix(l.File, "-stderr.txt"), Action: m[1], Lock: l.T}
			open[l.File+m[1]] = a
			res = append(res, a)
		} else if m := unlockRe.FindStringSubmatch(l.Text); m != nil && are.MatchString(m[2]) {
			if a := open[l.File+m[2]]; a != nil {
				a.Unlock, a.OK, a.Err = l.T, m[1] != "", m[3]
				delete(open, l.File+m[2])
			}
		}
	}
	var r []reparentAttempt
	for _, a := range res {
		r = append(r, *a)
	}
	return r
}

// initAttempts returns when a generation's log shows replication initialization asking for the
// shard lock.
func initAttempts(logFile string) []time.Time {
	var r []time.Time
	for _, l := range logMatches([]string{logFile}, regexp.MustCompile(`Locking shard \S+ for action initialize replication`), time.Time{}) {
		r = append(r, l.T)
	}
	return r
}

// ---- the experiment's run ----

type abRun struct {
	*Scenario
	p, r1, r2, r3, r4 *Node
	rp                *replPoller
	sups              []*Supervisor
	actionRe          string
	revokedAt         time.Time
	revokeTimedOut    bool
	firstAttempt      time.Time
	successAt         time.Time
	barrier           []*sql.Conn
	// cleanDemotion: the reparent demotes P itself (PRS), so a replica still replicating from P
	// after the promotion is stale but cannot revive P's writes.
	cleanDemotion bool
}

func (s *Scenario) newABRun(actionRe string) *abRun {
	r := &abRun{Scenario: s, actionRe: actionRe, p: s.node("zone1-0000000100"), r1: s.node("zone2-0000000200"), r2: s.node("zone3-0000000300")}
	if len(s.Nodes) == 5 {
		r.r3, r.r4 = s.node("zone2-0000000400"), s.node("zone3-0000000500")
	}
	policy, err := selectedRestartPolicy()
	if err != nil {
		s.t.Fatal(err)
	}
	s.R.outcome("A/B: supervisor policy %s; P=%s R1=%s R2=%s R3=%s R4=%s", policy.Name, aliasOf(r.p), aliasOf(r.r1), aliasOf(r.r2), aliasOf(r.r3), aliasOf(r.r4))
	s.ensurePrimary(r.p)
	r.rp = s.startReplPoller()
	return r
}

// holdBarrier holds the appliers of the given candidates (LOCK TABLES ... READ), so that a
// reparent waits for their relay logs while it holds the shard lock.
func (r *abRun) holdBarrier(ns ...*Node) {
	for _, n := range ns {
		r.barrier = append(r.barrier, r.lockTable(n))
	}
}

func (r *abRun) releaseBarrier() {
	for _, c := range r.barrier {
		if _, err := c.ExecContext(context.Background(), "unlock tables"); err != nil {
			r.R.violation("HARNESS: unlock tables: %v", err)
		}
		_ = c.Close()
	}
	if len(r.barrier) > 0 {
		r.Log.Add("heal", "barrier released (unlock tables)")
	}
	r.barrier = nil
}

// blockPrimary makes P's commits wait for semi-sync acknowledgements by cutting every replica
// from P's MySQL port.
func (r *abRun) blockPrimary() {
	for _, n := range r.Nodes {
		if n != r.p {
			if err := r.Net.BlockPorts(n.Group, r.p.Tablet.MySQLPort); err != nil {
				r.t.Fatal(err)
			}
		}
	}
	r.Log.Add("fault", "cut every replica -> P mysql (P's commits wait for semi-sync)")
	if _, ok := r.WaitFor("P has commits waiting for semi-sync", 15*time.Second, func() bool {
		v, _ := strconv.Atoi(r.p.status("Rpl_semi_sync_source_wait_sessions")["Rpl_semi_sync_source_wait_sessions"])
		return v > 0
	}); !ok {
		r.R.violation("HARNESS: P's commits never blocked on semi-sync")
	}
}

// startReparent runs a vtctldclient reparent command in the background.
func (r *abRun) startReparent(command string, args ...string) <-chan ersResult {
	argv := append([]string{"--server", r.CI.VtctldClientProcess.Server, command, keyspaceName + "/" + shardName}, args...)
	ch := make(chan ersResult, 1)
	start := time.Now()
	if r.firstAttempt.IsZero() {
		r.firstAttempt = start
	}
	r.Log.Add("reparent", "START vtctldclient "+strings.Join(argv[2:], " "))
	go func() {
		out, err := exec.Command(r.CI.VtctldClientProcess.Binary, argv...).CombinedOutput()
		res := ersResult{out: string(out), err: err, start: start, end: time.Now()}
		r.Log.Add("reparent", fmt.Sprintf("%s DONE after %.1fs err=%v", command, res.end.Sub(start).Seconds(), err))
		d := path.Join(resultsDir(), r.R.Name)
		_ = os.MkdirAll(d, 0o755)
		f, _ := os.OpenFile(path.Join(d, "reparent-output.txt"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if f != nil {
			fmt.Fprintf(f, "==== %s %s started %s err=%v\n%s\n", command, strings.Join(args, " "), start.Format("15:04:05.000"), err, out)
			f.Close()
		}
		ch <- res
	}()
	return ch
}

// waitRevoked waits until the given replicas' receivers are stopped.
func (r *abRun) waitRevoked(ns ...*Node) {
	_, ok := r.WaitFor("receivers stopped by the reparent", 60*time.Second, func() bool {
		for _, n := range ns {
			if r.ioRunning(n) != "No" {
				return false
			}
		}
		return true
	})
	if !ok {
		r.R.violation("HARNESS: the reparent did not stop the receivers of %v", aliases(ns))
		r.revokeTimedOut = true
	}
	r.revokedAt = time.Now()
}

// restart puts n under a supervisor (killing its vttablet) and waits until the new process has
// reached its initialization outcome: it deferred, it initialized, it exited, or (main, which
// takes no shard lock) it finished starting.
func (r *abRun) restart(n *Node, restore bool, prepare func(), timeout time.Duration) *Supervisor {
	sv := r.Supervise(n, restore, prepare)
	outcome := regexp.MustCompile(`deferring replication initialization|Unlocking shard \S+ for (successful )?action initialize replication|cannot initialize replication|In tmState\.Open\(\)`)
	_, ok := r.WaitFor(n.Tablet.Alias+" reached its initialization outcome", timeout, func() bool {
		gens := sv.Generations()
		g := gens[len(gens)-1]
		if g.N == 0 {
			return false
		}
		if g.N > 1 || !g.End.IsZero() {
			return true
		}
		return len(logMatches([]string{g.Log}, outcome, time.Time{})) > 0
	})
	if !ok {
		r.R.note("%s: no initialization outcome within %v", n.Tablet.Alias, timeout)
	}
	return sv
}

// abConvergenceProblems is convergenceProblems, except that with excludeOld the old primary is
// left out: an ERS against a written, blocked primary leaves it with errant GTIDs, so it cannot
// rejoin, whichever design runs. It then requires a new primary, and every other tablet to serve
// as a REPLICA of it.
func (r *abRun) abConvergenceProblems(excludeOld bool) []string {
	if !excludeOld {
		_, probs := r.convergenceProblems()
		return probs
	}
	p := r.topoPrimary()
	if p == nil || p == r.p {
		return []string{"the shard record does not name a new primary: " + aliasOf(p)}
	}
	var probs []string
	if typ := r.tabletTypeHTTP(p); typ != "PRIMARY" {
		probs = append(probs, fmt.Sprintf("primary %s vttablet type %s", p.Tablet.Alias, typ))
	}
	for _, n := range r.Nodes {
		if n == p || n == r.p {
			continue
		}
		if st, typ := tabletState(n), r.tabletTypeHTTP(n); st != "SERVING" || typ != "REPLICA" {
			probs = append(probs, fmt.Sprintf("%s %s %s", n.Tablet.Alias, typ, st))
		}
		if !r.replicatingFrom(n, p) {
			probs = append(probs, fmt.Sprintf("%s not replicating from %s", n.Tablet.Alias, p.Tablet.Alias))
		}
	}
	return probs
}

// waitConverged polls until the cluster has converged or the observation window ends, and
// reports the result as a duration from the first attempt or as censored.
func (r *abRun) waitConverged(from time.Time, excludeOld bool) {
	deadline := from.Add(abObservationWindow)
	var probs []string
	for time.Now().Before(deadline) {
		probs = r.abConvergenceProblems(excludeOld)
		if len(probs) == 0 {
			r.R.outcome("CONVERGED %.1fs after the first attempt", time.Since(from).Seconds())
			return
		}
		time.Sleep(time.Second)
	}
	r.R.outcome("CENSORED: not converged %v after the first attempt; state: %s", abObservationWindow, strings.Join(probs, "; "))
}

// analyze reports the reparent attempts, the overlap evidence, the restarts, the connections
// to P by phase, serving without replication, and the write outage.
func (r *abRun) analyze() {
	var restarted []*Node
	for _, sv := range r.sups {
		restarted = append(restarted, sv.n)
	}
	since := r.Fault.Add(-time.Second)
	attempts := r.reparentAttempts(r.actionRe, since)
	for _, a := range attempts {
		r.R.outcome("attempt %s %q lock %s unlock %s ok=%v err=%q", a.Who, a.Action, a.Lock.Format("15:04:05.000"), fmtTime(a.Unlock), a.OK, a.Err)
		if a.OK && r.successAt.IsZero() {
			r.successAt = a.Unlock
		}
	}
	r.R.outcome("reparent attempts=%d failed=%d; time from the first attempt to the first success: %s", len(attempts),
		len(slices.DeleteFunc(slices.Clone(attempts), func(a reparentAttempt) bool { return a.OK || a.Unlock.IsZero() })), sinceFirst(r.firstAttempt, r.successAt))

	// Overlap: each restarted tablet's first initialization attempt (or, without one, its first
	// generation's start) inside a reparent's lock hold, after its receiver was revoked.
	for i, sv := range r.sups {
		gens := sv.Generations()
		if len(gens) < 2 {
			continue
		}
		at, what := gens[1].Start, "generation 1 start (no initialization lock request logged)"
		if ia := initAttempts(gens[1].Log); len(ia) > 0 {
			at, what = ia[0], "generation 1 initialization lock request"
		}
		in := false
		for _, a := range attempts {
			end := a.Unlock
			if end.IsZero() {
				end = time.Now()
			}
			if !at.Before(a.Lock) && !at.After(end) {
				in = true
			}
		}
		counted := in && !r.revokeTimedOut && (r.revokedAt.IsZero() || !at.Before(r.revokedAt))
		r.R.outcome("OVERLAP %s: %s at %s, revoked at %s (wait timed out=%v), inside a reparent lock hold=%v -> counted=%v", restarted[i].Tablet.Alias, what,
			at.Format("15:04:05.000"), fmtTime(r.revokedAt), r.revokeTimedOut, in, counted)
		if !counted {
			r.R.violation("HARNESS: %s's restart did not overlap a reparent's lock hold after revocation; do not count this run", restarted[i].Tablet.Alias)
		}
		r.R.outcome("RESTARTS %s: exits by itself=%d; %s", restarted[i].Tablet.Alias, sv.SelfExits(), sv.Summary())
		sv.Check()
	}

	// Connections to P, by phase.
	samples := r.rp.Samples()
	pPort := strconv.Itoa(r.p.Tablet.MySQLPort)
	revoked := map[string]time.Time{}
	for _, smp := range samples {
		if _, ok := revoked[smp.Node]; !ok && !r.firstAttempt.IsZero() && smp.T.After(r.firstAttempt) && (smp.IO != "Yes" || smp.Port != pPort) {
			revoked[smp.Node] = smp.T
		}
	}
	type phaseCount struct {
		n           int
		first, last time.Time
	}
	phases := map[string]*phaseCount{}
	for _, smp := range samples {
		if smp.Node == r.p.Tablet.Alias || smp.IO != "Yes" || smp.Port != pPort || smp.T.Before(since) {
			continue
		}
		var ph string
		rv, ok := revoked[smp.Node]
		switch {
		case !ok || smp.T.Before(rv):
			ph = "a"
		case !r.successAt.IsZero() && !smp.T.Before(r.successAt):
			ph = "d"
		case inAttempt(attempts, smp.T):
			ph = "b"
		default:
			ph = "c"
		}
		k := smp.Node + " phase " + ph
		if phases[k] == nil {
			phases[k] = &phaseCount{first: smp.T}
		}
		phases[k].n++
		phases[k].last = smp.T
	}
	keys := slices.Sorted(func(yield func(string) bool) {
		for k := range phases {
			if !yield(k) {
				return
			}
		}
	})
	for _, k := range keys {
		pc := phases[k]
		line := fmt.Sprintf("CONNECTED TO P %s: %d samples, %s to %s%s", k, pc.n, pc.first.Format("15:04:05.000"), pc.last.Format("15:04:05.000"), r.generationAt(k, pc.first))
		if strings.HasSuffix(k, "phase b") || (strings.HasSuffix(k, "phase d") && !r.cleanDemotion) {
			r.R.violation("SAFETY: %s", line)
		} else {
			r.R.outcome("%s", line)
		}
	}

	// Serving without replication: a restarted tablet reports SERVING while its receiver is not
	// running, for more than 2s outside a reparent's lock hold.
	for _, n := range restarted {
		var start, last time.Time
		flush := func() {
			if !start.IsZero() && last.Sub(start) > 2*time.Second {
				r.R.outcome("SERVED WITHOUT REPLICATION %s: %s to %s (%.1fs)", n.Tablet.Alias, start.Format("15:04:05.000"), last.Format("15:04:05.000"), last.Sub(start).Seconds())
			}
			start = time.Time{}
		}
		for _, smp := range samples {
			if smp.Node != n.Tablet.Alias || smp.T.Before(since) {
				continue
			}
			if smp.Status == "SERVING" && smp.IO != "Yes" && !inAttempt(attempts, smp.T) {
				if start.IsZero() {
					start = smp.T
				}
				last = smp.T
			} else {
				flush()
			}
		}
		flush()
	}

	// Write outage: the longest gap between acknowledged writes from the fault on.
	var acks []time.Time
	for _, w := range r.W.Records() {
		if w.Acked && w.End.After(r.Fault) {
			acks = append(acks, w.End)
		}
	}
	slices.SortFunc(acks, func(a, b time.Time) int { return a.Compare(b) })
	prev, gap, gapAt := r.Fault, time.Duration(0), r.Fault
	for _, a := range acks {
		if a.Sub(prev) > gap {
			gap, gapAt = a.Sub(prev), prev
		}
		prev = a
	}
	r.R.outcome("WRITES: %d acked after the fault; longest gap %.1fs from %s", len(acks), gap.Seconds(), gapAt.Format("15:04:05.000"))

	b, _ := json.MarshalIndent(struct {
		Fault, FirstAttempt, RevokedAt, SuccessAt time.Time
		Attempts                                  []reparentAttempt
		Revoked                                   map[string]time.Time
	}{r.Fault, r.firstAttempt, r.revokedAt, r.successAt, attempts, revoked}, "", "  ")
	d := path.Join(resultsDir(), r.R.Name)
	_ = os.MkdirAll(d, 0o755)
	_ = os.WriteFile(path.Join(d, "ab-timeline.json"), b, 0o644)
}

// generationAt names the supervised generation of the phase key's tablet running at t.
func (r *abRun) generationAt(key string, t time.Time) string {
	for _, sv := range r.sups {
		if !strings.HasPrefix(key, sv.n.Tablet.Alias+" ") {
			continue
		}
		for _, g := range sv.Generations() {
			if g.N > 0 && !t.Before(g.Start) && (g.End.IsZero() || !t.After(g.End)) {
				return fmt.Sprintf(" (generation %d)", g.N)
			}
		}
		return " (between generations)"
	}
	return ""
}

func inAttempt(attempts []reparentAttempt, t time.Time) bool {
	for _, a := range attempts {
		if !t.Before(a.Lock) && (a.Unlock.IsZero() || !t.After(a.Unlock)) {
			return true
		}
	}
	return false
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format("15:04:05.000")
}

func sinceFirst(first, t time.Time) string {
	if first.IsZero() || t.IsZero() {
		return "none"
	}
	return fmt.Sprintf("%.1fs", t.Sub(first).Seconds())
}

// ---- scenarios ----

var abFiveTablets = Options{ExtraTabletCells: []string{"zone2", "zone3"}}

// abERS is E1 (manual ERS), E1-vtorc (VTOrc's ERS) and, without restarts, E4.
func abERS(t *testing.T, name string, viaVTOrc, restarts bool) {
	runScenario(t, name, abFiveTablets, func(s *Scenario) {
		r := s.newABRun(map[bool]string{false: ersAction, true: vtorcAction}[viaVTOrc])
		s.setRecoveries(viaVTOrc)
		s.Sleep(3*time.Second, "baseline")
		if restarts {
			// R3 and R4 keep receiving but stop applying, so the ERS waits for their relay logs.
			r.holdBarrier(r.r3, r.r4)
			s.Sleep(3*time.Second, "R3 and R4 receive without applying")
		}
		r.blockPrimary()
		s.MarkFault()
		var ers <-chan ersResult
		if viaVTOrc {
			if s.WaitForOrcLog(`Locking shard `+keyspaceName+"/"+shardName+` for action `+vtorcAction, s.Fault, 120*time.Second) == nil {
				s.R.violation("HARNESS: VTOrc did not start a recovery within 120s")
			}
			r.firstAttempt = time.Now()
		} else {
			ers = r.startReparent("EmergencyReparentShard", "--new-primary", r.r3.Tablet.Alias, "--wait-replicas-timeout", "120s")
		}
		r.waitRevoked(r.r1, r.r2)

		if restarts {
			// A receiver that a restarted tablet starts can reach P again.
			for _, n := range []*Node{r.r1, r.r2} {
				if err := s.Net.Unblock(n.Group, r.p.Tablet.MySQLPort); err != nil {
					t.Fatal(err)
				}
			}
			s.Log.Add("heal", "R1, R2 -> P mysql reachable again")
			outcomeTimeout := 30 * time.Second
			if viaVTOrc {
				// VTOrc's ERS waits for relay logs only up to its own --wait-replicas-timeout.
				outcomeTimeout = 8 * time.Second
			}
			sups := make([]*Supervisor, 2)
			var wg sync.WaitGroup
			for i, n := range []*Node{r.r1, r.r2} {
				wg.Go(func() { sups[i] = r.restart(n, false, nil, outcomeTimeout) })
			}
			wg.Wait()
			r.sups = append(r.sups, sups...)
			r.releaseBarrier()
		}

		if viaVTOrc {
			s.WaitFor("topo primary is not P", abObservationWindow-time.Since(r.firstAttempt), s.PrimaryChanged(r.p))
		} else {
			res, done := s.waitERS(ers, 240*time.Second)
			if !done {
				s.R.violation("HARNESS: ERS did not finish in 240s")
			}
			for done && res.err != nil && time.Since(r.firstAttempt) < abObservationWindow {
				s.Sleep(10*time.Second, "ERS failed; retrying")
				res, done = s.waitERS(r.startReparent("EmergencyReparentShard", "--new-primary", r.r3.Tablet.Alias, "--wait-replicas-timeout", "20s"), 240*time.Second)
			}
		}
		s.R.outcome("topo primary after the reparent phase: %v; vttablet types: %s", aliasOf(s.topoPrimary()), s.types())
		s.Heal()
		if !viaVTOrc {
			s.setRecoveries(true)
		}
		r.waitConverged(r.firstAttempt, true)
		r.analyze()
		s.reportErrantDetail("at the end")
	})
}

// E1: R1 and R2 restart while a manual ERS against a reachable, semi-sync-blocked P holds the
// shard lock.
func TestABE1ManualERS(t *testing.T) {
	abERS(t, "ABE1-manual-ers-"+abSuffix(), false, true)
}

// E1-vtorc: as E1, with VTOrc running the ERS.
func TestABE1VTOrcERS(t *testing.T) {
	abERS(t, "ABE1vtorc-vtorc-ers-"+abSuffix(), true, true)
}

// E4: the manual ERS of E1 without restarts or barrier.
func TestABE4ControlERS(t *testing.T) {
	abERS(t, "ABE4-control-ers-"+abSuffix(), false, false)
}

// E2: R1, then R2, restart while a PRS to R3 holds the shard lock.
func TestABE2PRS(t *testing.T) {
	runScenario(t, "ABE2-prs-"+abSuffix(), abFiveTablets, func(s *Scenario) {
		r := s.newABRun(prsAction)
		r.cleanDemotion = true
		s.setRecoveries(true)
		s.Sleep(3*time.Second, "baseline")
		r.holdBarrier(r.r3)
		s.Sleep(3*time.Second, "R3 receives without applying")
		s.MarkFault()
		prs := r.startReparent("PlannedReparentShard", "--new-primary", r.r3.Tablet.Alias, "--wait-replica-timeout", "120s")
		if _, ok := s.WaitFor("P demoted by the PRS", 60*time.Second, func() bool {
			v, err := r.p.scalar("select @@global.super_read_only")
			return err == nil && v == "1"
		}); !ok {
			s.R.violation("HARNESS: the PRS did not demote P")
		}
		r.revokedAt = time.Now()
		r.sups = append(r.sups, r.restart(r.r1, false, nil, 30*time.Second))
		r.sups = append(r.sups, r.restart(r.r2, false, nil, 30*time.Second))
		r.releaseBarrier()
		res, done := s.waitERS(prs, 240*time.Second)
		s.R.outcome("PRS done=%v took %.1fs err=%v", done, res.end.Sub(res.start).Seconds(), res.err)
		s.R.outcome("topo primary after the PRS: %v; vttablet types: %s", aliasOf(s.topoPrimary()), s.types())
		r.waitConverged(r.firstAttempt, false)
		r.analyze()
		s.reportErrantDetail("at the end")
	})
}

// E3: R1 restores at startup (--restore-from-backup on an empty keyspace database) while the
// harness holds the shard lock, which it releases 5s after R1's finalization starts.
func TestABE3StartupRestore(t *testing.T) {
	runScenario(t, "ABE3-startup-restore-"+abSuffix(), Options{}, func(s *Scenario) {
		r := s.newABRun(`E3 harness hold`)
		bStart := time.Now()
		out, err := s.CI.VtctldClientProcess.ExecuteCommandWithOutput("Backup", r.r2.Tablet.Alias)
		s.Log.Add("setup", fmt.Sprintf("Backup of R2 took %.1fs err=%v", time.Since(bStart).Seconds(), err))
		if err != nil {
			t.Fatalf("Backup %s: %v\n%s", r.r2.Tablet.Alias, err, out)
		}
		s.WaitHealthy(90 * time.Second)
		// VTOrc would wait on the harness's lock; it is enabled again after the release.
		s.setRecoveries(false)
		s.Sleep(3*time.Second, "baseline")

		_, unlock, err := s.Ts.LockShard(t.Context(), keyspaceName, shardName, "E3 harness hold")
		if err != nil {
			t.Fatal(err)
		}
		lockedAt := time.Now()
		s.Log.Add("fault", "harness holds the shard lock")
		s.MarkFault()
		r.firstAttempt = lockedAt
		sv := r.Supervise(r.r1, true, func() {
			for _, q := range []string{
				"stop replica", "set global super_read_only = 0", "set session sql_log_bin = 0",
				"drop database if exists vt_" + keyspaceName, "set global super_read_only = 1",
			} {
				if _, err := r.r1.db.Exec(q); err != nil {
					t.Fatalf("%s on R1: %v", q, err)
				}
			}
			s.Log.Add("fault", "R1: stopped replication, dropped vt_"+keyspaceName+" without binlogging")
		})
		r.sups = append(r.sups, sv)
		finalizing := regexp.MustCompile(`Restore: starting replication at position`)
		var finalizeAt time.Time
		if _, ok := s.WaitFor("R1's restore finalization", 240*time.Second, func() bool {
			gens := sv.Generations()
			if l := logMatches([]string{gens[len(gens)-1].Log}, finalizing, lockedAt); len(l) > 0 {
				finalizeAt = l[0].T
				return true
			}
			return false
		}); !ok {
			s.R.violation("HARNESS: R1's restore never reached finalization")
		}
		s.Sleep(5*time.Second, "hold the lock 5s into the finalization")
		var uerr error
		unlock(&uerr)
		releasedAt := time.Now()
		s.Log.Add("fault", fmt.Sprintf("harness released the shard lock err=%v", uerr))
		s.R.outcome("harness lock held %s to %s; finalization started %s", lockedAt.Format("15:04:05.000"), releasedAt.Format("15:04:05.000"), fmtTime(finalizeAt))
		inHold := false
		for _, g := range sv.Generations() {
			for _, at := range initAttempts(g.Log) {
				if g.N == 1 && at.After(lockedAt) && at.Before(releasedAt) {
					inHold = true
				}
			}
		}
		s.R.outcome("OVERLAP R1: generation 1's finalization requested the shard lock inside the harness hold=%v -> counted=%v", inHold, inHold)
		if !inHold {
			s.R.violation("HARNESS: R1's finalization did not overlap the harness lock hold; do not count this run")
		}
		s.setRecoveries(true)
		r.waitConverged(releasedAt, false)
		tp := s.topoPrimary()
		s.R.outcome("R1 at the end: type=%s replicating from the shard primary %s=%v; %s", s.tabletTypeHTTP(r.r1), aliasOf(tp), tp != nil && s.replicatingFrom(r.r1, tp), sv.Summary())
		r.successAt = releasedAt
		r.analyzeRestore(sv)
		s.reportErrantDetail("at the end")
	})
}

// analyzeRestore reports E3's restarts and whether R1 served without replication.
func (r *abRun) analyzeRestore(sv *Supervisor) {
	r.R.outcome("RESTARTS %s: exits by itself=%d; %s", r.r1.Tablet.Alias, sv.SelfExits(), sv.Summary())
	sv.Check()
	var start, last time.Time
	report := func() {
		if !start.IsZero() {
			r.R.violation("SAFETY: R1 SERVING without a running receiver %s to %s (%.1fs)", start.Format("15:04:05.000"), last.Format("15:04:05.000"), last.Sub(start).Seconds())
		}
		start = time.Time{}
	}
	for _, smp := range r.rp.Samples() {
		if smp.Node != r.r1.Tablet.Alias || smp.T.Before(r.Fault) {
			continue
		}
		if smp.Status == "SERVING" && smp.IO != "Yes" {
			if start.IsZero() {
				start = smp.T
			}
			last = smp.T
		} else {
			report()
		}
	}
	report()
}

// abSuffix names a run after the supervisor policy.
func abSuffix() string {
	p, err := selectedRestartPolicy()
	if err != nil {
		return "invalid-policy"
	}
	return p.Name
}

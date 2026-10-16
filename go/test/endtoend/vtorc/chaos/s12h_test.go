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
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// S12h: local scenario for the fourth rerun of the #21549 validation.

// S12h: a replica's mysqld hangs while that replica's deferred replication initialization holds
// the shard lock. Layout: P = zone1-100, R1 = zone2-200 (restarted, its mysqld SIGSTOPped),
// R2 = zone3-300 (ERS target), R3 = zone2-400 (an acker for R2, so that ERS's semi-sync acker
// check does not fail just because P is cut and R1 is hung).
//  1. The harness takes the shard lock itself and restarts R1's vttablet, which defers.
//  2. After ~5s, R1's mysqld is SIGSTOPped and the harness releases the lock. R1's retry takes
//     the shard lock and reads replication status from the stopped mysqld.
//  3. P is cut from vtctld and the VTOrcs, and ERS --new-primary R2 runs.
//  4. After ERS ends, R1's mysqld is SIGCONTed.
func TestS12hHungMysqldHoldsInitLock(t *testing.T) {
	runScenario(t, "S12h-hung-mysqld-init-lock", Options{ExtraTabletCells: []string{"zone2"}}, func(s *Scenario) {
		p := s.node("zone1-0000000100")
		r1 := s.node("zone2-0000000200")
		r2 := s.node("zone3-0000000300")
		s.ensurePrimary(p)
		s.setRecoveries(false)
		s.R.outcome("P=%s R1=%s (restarted, mysqld SIGSTOPped) R2=%s (ERS target)", p.Tablet.Alias, r1.Tablet.Alias, r2.Tablet.Alias)
		s.Sleep(3*time.Second, "baseline")

		_, unlock, err := s.Ts.LockShard(t.Context(), keyspaceName, shardName, "S12h harness hold")
		if err != nil {
			t.Fatal(err)
		}
		s.Log.Add("fault", "harness holds the shard lock")
		s.restartVttablet(r1)
		s.Sleep(5*time.Second, "R1 defers while the harness holds the shard lock")

		pids := signalGroup(r1.Group, syscall.SIGSTOP, "mysqld")
		s.Log.Add("fault", fmt.Sprintf("SIGSTOP mysqld of %s pids=%v", r1.Tablet.Alias, pids))
		s.Sleep(200*time.Millisecond, "mysqld stopped")
		var uerr error
		unlock(&uerr)
		releasedAt := time.Now()
		s.Log.Add("fault", fmt.Sprintf("harness released the shard lock err=%v", uerr))
		// Let R1's retry tick (every 1s) take the lock and hang on the stopped mysqld.
		s.Sleep(1500*time.Millisecond, "R1's retry takes the shard lock")

		s.cutPrimaryFromERSCaller(p)
		s.MarkFault()
		ers := s.startERS("--new-primary", r2.Tablet.Alias)
		// Variant (S12H_SIGCONT_ON_UNLOCK=1, uncounted): resume R1's mysqld as soon as R1's log shows
		// its retry releasing the shard lock, so that ERS can reach R1. The scenario as specified
		// resumes it only after ERS ends, and then semi-sync ERS cannot promote anything while R1 is
		// unreachable ("could not reach sufficient tablets to guarantee safety").
		resumed := false
		if os.Getenv("S12H_SIGCONT_ON_UNLOCK") == "1" {
			s.R.outcome("variant: SIGCONT R1's mysqld when its retry releases the shard lock")
			for !resumed {
				select {
				case r := <-ers:
					ers = func() <-chan ersResult { c := make(chan ersResult, 1); c <- r; return c }()
					resumed = true
					continue
				case <-time.After(100 * time.Millisecond):
				}
				if t0, ok := logLineAfter(r1.Tablet.VttabletProcess.ErrorLog, "Unlocking shard ks/0 for action initialize replication", releasedAt); ok {
					pids := signalGroup(r1.Group, syscall.SIGCONT, "mysqld")
					s.Log.Add("heal", fmt.Sprintf("R1's retry released the shard lock at %s; SIGCONT mysqld of %s pids=%v", t0.Format("15:04:05.000"), r1.Tablet.Alias, pids))
					resumed = true
				}
			}
		}
		r, done := s.waitERS(ers, 240*time.Second)
		if !done {
			s.R.violation("HARNESS: ERS did not finish in 240s")
		} else {
			s.reportERS(r)
			s.R.outcome("ERS started %.1fs after the harness released the shard lock", r.start.Sub(releasedAt).Seconds())
		}
		s.R.outcome("topo primary right after ERS: %v; vttablet types: %s", aliasOf(s.topoPrimary()), s.types())

		pids = signalGroup(r1.Group, syscall.SIGCONT, "mysqld")
		s.Log.Add("heal", fmt.Sprintf("SIGCONT mysqld of %s pids=%v", r1.Tablet.Alias, pids))
		s.Sleep(10*time.Second, "R1 resumes; recoveries still off")
		rs, _ := r1.replicaStatus()
		s.R.outcome("R1 10s after SIGCONT (recoveries off): source_port=%s io=%s sql=%s type=%s", rs["Source_Port"], rs["Replica_IO_Running"],
			rs["Replica_SQL_Running"], s.tabletTypeHTTP(r1))
		s.R.note("R1 vttablet log: %s", s.grepTabletLog(r1, "initialize replication|killing connID|did not come back|Retrying deferred|Initialized deferred", 30))
		tagged := s.reportAckedSince("acked after fault", s.Fault, time.Now())
		s.afterERS(nil)
		rs, _ = r1.replicaStatus()
		s.R.outcome("R1 after settle: source_port=%s (topo primary %s) io=%s sql=%s", rs["Source_Port"], aliasOf(s.topoPrimary()), rs["Replica_IO_Running"], rs["Replica_SQL_Running"])
		s.reportErrantDetail("after settle")
		s.reportLostTagged("acked after fault (before invariant checks)", tagged)
	})
}

// logLineAfter reports the time of the first line of a Vitess log that contains substr and is
// stamped after since.
func logLineAfter(file, substr string, since time.Time) (time.Time, bool) {
	b, err := os.ReadFile(file)
	if err != nil {
		return time.Time{}, false
	}
	for l := range strings.SplitSeq(string(b), "\n") {
		if len(l) < 23 || !strings.Contains(l, substr) {
			continue
		}
		ts, err := time.Parse("2006-01-02 15:04:05.000", l[:23])
		if err == nil && ts.After(since.UTC()) {
			return ts, true
		}
	}
	return time.Time{}, false
}

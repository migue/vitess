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
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// S12f: a former primary whose commits are blocked on semi-sync restarts as a replica (local,
// #21549 final rerun). Layout: P = zone1-100 (primary), R1 = zone2-200, R2 = zone3-300.
//  1. R1 and R2 are cut from P's MySQL port, so P's commits block waiting for acknowledgements
//     (Rpl_semi_sync_source_wait_sessions > 0).
//  2. P's vttablet is killed at once (kill -9), before anything can demote it; P's mysqld keeps
//     running with the blocked commits.
//  3. ERS --new-primary R2; wait until the shard record names R2.
//  4. R1/R2 -> P is restored (ERS repointed R1 to R2, so nothing acknowledges P's commits).
//  5. P's vttablet is restarted without --restore-from-backup, so startup runs replication
//     initialization. S12F_MODE=retry holds the shard lock for 5s around the restart, so P defers
//     and the background retry initializes it.
func TestS12fFormerPrimaryBlockedCommits(t *testing.T) {
	mode := os.Getenv("S12F_MODE")
	if mode == "" {
		mode = "start"
	}
	runScenario(t, "S12f-former-primary-blocked-commits-"+mode, Options{}, func(s *Scenario) {
		p := s.node("zone1-0000000100")
		r1 := s.node("zone2-0000000200")
		r2 := s.node("zone3-0000000300")
		s.ensurePrimary(p)
		s.setRecoveries(false)
		s.R.outcome("P=%s (former primary, vttablet killed and restarted) R1=%s R2=%s (ERS target) mode=%s", p.Tablet.Alias, r1.Tablet.Alias, r2.Tablet.Alias, mode)
		s.Sleep(3*time.Second, "baseline")
		pState := func(label string) {
			v := p.variables("%read_only")
			ss := p.variables("rpl_semi_sync_source_enabled")
			st := p.status("Rpl_semi_sync_source_wait_sessions")
			g, _ := p.gtidExecuted()
			s.R.outcome("P %s: super_read_only=%s read_only=%s rpl_semi_sync_source_enabled=%s wait_sessions=%s gtid_executed=%s", label,
				v["super_read_only"], v["read_only"], ss["rpl_semi_sync_source_enabled"], st["Rpl_semi_sync_source_wait_sessions"], strings.ReplaceAll(g, "\n", ""))
		}

		// 1. Block P's commits on semi-sync.
		for _, n := range []*Node{r1, r2} {
			if err := s.Net.BlockPorts(n.Group, p.Tablet.MySQLPort); err != nil {
				t.Fatal(err)
			}
		}
		s.Log.Add("fault", "cut R1, R2 -> P mysql (P's commits wait for semi-sync)")
		s.MarkFault()
		d, ok := s.WaitFor("P has commits waiting for semi-sync", 10*time.Second, func() bool {
			v, _ := strconv.Atoi(p.status("Rpl_semi_sync_source_wait_sessions")["Rpl_semi_sync_source_wait_sessions"])
			return v > 0
		})
		if !ok {
			s.R.violation("HARNESS: P's commits never blocked on semi-sync")
		}
		// 2. Kill P's vttablet at once.
		vp := p.Tablet.VttabletProcess
		if vp.ErrorLog != "" {
			b, _ := os.ReadFile(vp.ErrorLog)
			_ = os.WriteFile(strings.TrimSuffix(vp.ErrorLog, ".txt")+"-before-kill.txt", b, 0o644)
		}
		_ = vp.Kill()
		s.Log.Add("fault", fmt.Sprintf("kill -9 vttablet of %s (%.2fs after the cut)", p.Tablet.Alias, d.Seconds()))
		pState("after its vttablet was killed")
		killedAt := time.Now()

		// 3. ERS to R2, then wait for the shard record.
		s.cutPrimaryFromERSCaller(p)
		r, done := s.waitERS(s.startERS("--new-primary", r2.Tablet.Alias), 240*time.Second)
		if !done {
			s.R.violation("HARNESS: ERS did not finish in 240s")
		} else {
			s.reportERS(r)
		}
		d, ok = s.WaitFor("shard record names R2", 60*time.Second, func() bool { return s.topoPrimary() == r2 })
		s.R.outcome("shard record names R2: %v (%.1fs after ERS end); vttablet types: %s", ok, d.Seconds(), s.types())

		// 4. Restore R1/R2 -> P, and the ERS caller's view of P.
		for _, n := range []*Node{r1, r2} {
			if err := s.Net.Unblock(n.Group, p.Tablet.MySQLPort); err != nil {
				t.Fatal(err)
			}
		}
		s.Heal()
		s.Log.Add("heal", "R1, R2 -> P mysql reachable again; network healed")
		s.Sleep(2*time.Second, "network restored")
		pState("before its vttablet restarts")
		r2g, _ := r2.gtidExecuted()

		// 5. Restart P's vttablet.
		var unlock func(*error)
		if mode == "retry" {
			_, u, err := s.Ts.LockShard(t.Context(), keyspaceName, shardName, "S12f harness hold")
			if err != nil {
				t.Fatal(err)
			}
			unlock = u
			s.Log.Add("fault", "harness holds the shard lock")
		}
		vp.SupportsBackup = os.Getenv("S12_RESTORE_FROM_BACKUP") == "1"
		vp.ServingStatus = ""
		err := vp.Setup()
		restartAt := time.Now()
		s.Log.Add("fault", fmt.Sprintf("vttablet of %s started again err=%v", p.Tablet.Alias, err))
		if unlock != nil {
			s.Sleep(5*time.Second, "P defers while the harness holds the shard lock")
			var uerr error
			unlock(&uerr)
			s.Log.Add("fault", fmt.Sprintf("harness released the shard lock err=%v", uerr))
		}
		s.Sleep(15*time.Second, "P's replication initialization")
		pState("15s after its vttablet restarted")
		rs, _ := p.replicaStatus()
		s.R.outcome("P replication: source_port=%s (R2 %d) io=%s sql=%s; vttablet=%s", rs["Source_Port"], r2.Tablet.MySQLPort, rs["Replica_IO_Running"],
			rs["Replica_SQL_Running"], s.tabletTypeHTTP(p))
		for _, n := range []*Node{r1, r2} {
			reps, _ := n.query("SHOW REPLICAS")
			var ids []string
			for _, row := range reps {
				ids = append(ids, row["Server_Id"])
			}
			s.R.outcome("%s SHOW REPLICAS server_ids=%v (P server_id %s)", n.Tablet.Alias, ids, p.variables("server_id")["server_id"])
		}
		pg, _ := p.gtidExecuted()
		extra, _ := r2.scalar("select gtid_subtract(?, ?)", pg, r2g)
		r1g, _ := r1.gtidExecuted()
		r2now, _ := r2.gtidExecuted()
		onR2, _ := r2.scalar("select gtid_subtract(?, gtid_subtract(?, ?))", extra, extra, r2now)
		onR1, _ := r2.scalar("select gtid_subtract(?, gtid_subtract(?, ?))", extra, extra, r1g)
		s.R.outcome("P gtid_executed minus R2's (released commits): %q; of these on R2 now: %q, on R1 now: %q", strings.ReplaceAll(extra, "\n", ""),
			strings.ReplaceAll(onR2, "\n", ""), strings.ReplaceAll(onR1, "\n", ""))
		s.R.note("P vttablet log: %s", strings.ReplaceAll(s.grepTabletLog(p, "Disabling source-side semi-sync|rrant|initialize replication|deferr|Retrying deferred|Initialized deferred|CHANGE REPLICATION SOURCE|failed to parse", 30), "\n", " || "))

		// Rows on P that R2 lacks, and what their clients were told.
		pIDs, _ := p.ids()
		r2IDs, _ := r2.ids()
		var only []int64
		for id := range pIDs {
			if _, ok := r2IDs[id]; !ok {
				only = append(only, id)
			}
		}
		sort.Slice(only, func(i, j int) bool { return only[i] < only[j] })
		recs := map[int64]WriteRecord{}
		for _, rec := range s.W.Records() {
			recs[rec.ID] = rec
		}
		acked := 0
		for _, id := range only {
			if recs[id].Acked {
				acked++
			}
		}
		s.R.outcome("rows on P not on R2: %d %v; acknowledged to their clients: %d", len(only), firstN(only, 20), acked)
		failed := 0
		for _, rec := range s.W.Records() {
			if !rec.Acked && rec.Start.After(s.Fault.Add(-time.Second)) && rec.Start.Before(killedAt) {
				failed++
			}
		}
		s.R.outcome("writes started between the cut and the kill that failed for their clients: %d", failed)
		_ = restartAt

		// Let the cluster settle: P is restarted again if its vttablet is down.
		// Keep the log of the restart under test: Setup truncates it.
		if b, err := os.ReadFile(vp.ErrorLog); err == nil {
			_ = os.WriteFile(strings.TrimSuffix(vp.ErrorLog, ".txt")+"-after-restart.txt", b, 0o644)
		}
		if s.tabletTypeHTTP(p) == "DOWN" {
			_ = vp.Kill()
			vp.ServingStatus = ""
			err := vp.Setup()
			s.Log.Add("heal", fmt.Sprintf("P's vttablet was down; started again err=%v", err))
		}
		tagged := s.reportAckedSince("acked after fault", s.Fault, time.Now())
		s.afterERS(nil)
		s.reportErrantDetail("after settle")
		s.reportLostTagged("acked after fault (before invariant checks)", tagged)
	})
}

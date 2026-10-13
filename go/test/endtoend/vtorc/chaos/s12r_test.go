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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

// S12r: a replica's restore finalization runs while an EmergencyReparentShard holds the shard
// lock.
//
// Layout: P = zone1-100 (made primary with PRS if needed), R1 = zone2-200 (restored),
// R2 = zone3-300 (backed up before the fault). VTOrc recoveries are disabled; the only failover
// is a manual ERS through vtctld, with P unreachable from vtctld and the VTOrcs only.
//
// R1's restore holds R1's tablet action lock from its start. While it runs, the ERS takes the
// shard lock and calls StopReplicationAndGetStatus on R1, which waits for that action lock, so
// the restore's finalization (connecting the restored data to the shard primary) runs while the
// ERS holds the shard lock. With viaStartup the restore is started by restarting R1's vttablet
// with --restore-from-backup on an empty keyspace database instead of the RestoreFromBackup RPC.

type restoreResult struct {
	out        string
	err        error
	start, end time.Time
}

// latestManifest returns the newest backup's directory and its MANIFEST position.
func (s *Scenario) latestManifest(n *Node) (string, string) {
	dir := path.Join(n.Tablet.VttabletProcess.FileBackupStorageRoot, keyspaceName, shardName)
	entries, err := os.ReadDir(dir)
	if err != nil {
		s.R.violation("HARNESS: cannot list backups in %s: %v", dir, err)
		return "", ""
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	if len(names) == 0 {
		s.R.violation("HARNESS: no backup in %s", dir)
		return "", ""
	}
	sort.Strings(names)
	b, err := os.ReadFile(path.Join(dir, names[len(names)-1], "MANIFEST"))
	if err != nil {
		s.R.violation("HARNESS: cannot read MANIFEST: %v", err)
		return names[len(names)-1], ""
	}
	var m struct{ Position string }
	if err := json.Unmarshal(b, &m); err != nil {
		s.R.violation("HARNESS: cannot parse MANIFEST: %v", err)
	}
	return names[len(names)-1], m.Position
}

func s12r(t *testing.T, name string, viaStartup bool) {
	runScenario(t, name, Options{}, func(s *Scenario) {
		p := s.node("zone1-0000000100")
		r1 := s.node("zone2-0000000200")
		r2 := s.node("zone3-0000000300")
		s.ensurePrimary(p)
		s.R.outcome("P=%s R1=%s (restored, via %s) R2=%s (backed up)", p.Tablet.Alias, r1.Tablet.Alias,
			map[bool]string{true: "vttablet startup --restore-from-backup", false: "RestoreFromBackup RPC"}[viaStartup], r2.Tablet.Alias)

		bStart := time.Now()
		out, err := s.CI.VtctldClientProcess.ExecuteCommandWithOutput("Backup", r2.Tablet.Alias)
		s.Log.Add("setup", fmt.Sprintf("Backup of R2 took %.1fs err=%v", time.Since(bStart).Seconds(), err))
		if err != nil {
			t.Fatalf("Backup %s: %v\n%s", r2.Tablet.Alias, err, out)
		}
		s.WaitHealthy(90 * time.Second)
		backup, manifestPos := s.latestManifest(r2)
		s.R.outcome("backup %s MANIFEST position %s", backup, manifestPos)
		s.setRecoveries(false)
		s.Sleep(3*time.Second, "baseline")

		// Start R1's restore.
		restoreCh := make(chan restoreResult, 1)
		if viaStartup {
			// An empty keyspace database makes vttablet --restore-from-backup restore at startup.
			vp := r1.Tablet.VttabletProcess
			if vp.ErrorLog != "" {
				b, _ := os.ReadFile(vp.ErrorLog)
				_ = os.WriteFile(strings.TrimSuffix(vp.ErrorLog, ".txt")+"-before-restart.txt", b, 0o644)
			}
			_ = vp.Kill()
			s.Log.Add("fault", "kill -9 vttablet of "+r1.Tablet.Alias)
			for _, q := range []string{"stop replica", "set global super_read_only = 0", "set session sql_log_bin = 0",
				"drop database if exists vt_" + keyspaceName, "set global super_read_only = 1"} {
				if _, err := r1.db.Exec(q); err != nil {
					t.Fatalf("%s on R1: %v", q, err)
				}
			}
			s.Log.Add("fault", "R1: stopped replication, dropped vt_"+keyspaceName+" without binlogging")
			vp.SupportsBackup = true
			vp.ServingStatus = ""
			rStart := time.Now()
			err := vp.Setup()
			s.Log.Add("fault", fmt.Sprintf("vttablet of %s started with --restore-from-backup err=%v", r1.Tablet.Alias, err))
			go func() {
				// The startup restore has no RPC result: it is done when R1 leaves RESTORE.
				seen := false
				for time.Since(rStart) < 180*time.Second {
					tt := s.tabletTypeHTTP(r1)
					if tt == "RESTORE" {
						seen = true
					} else if seen && tt != "DOWN" {
						restoreCh <- restoreResult{out: "tablet type " + tt, start: rStart, end: time.Now()}
						return
					}
					time.Sleep(50 * time.Millisecond)
				}
				restoreCh <- restoreResult{err: fmt.Errorf("restore did not finish (seen RESTORE=%v)", seen), start: rStart, end: time.Now()}
			}()
		} else {
			rStart := time.Now()
			s.Log.Add("fault", "START vtctldclient RestoreFromBackup "+r1.Tablet.Alias)
			go func() {
				out, err := exec.Command(s.CI.VtctldClientProcess.Binary, "--server", s.CI.VtctldClientProcess.Server,
					"RestoreFromBackup", r1.Tablet.Alias).CombinedOutput()
				r := restoreResult{out: string(out), err: err, start: rStart, end: time.Now()}
				s.Log.Add("restore", fmt.Sprintf("RestoreFromBackup DONE after %.1fs err=%v", r.end.Sub(rStart).Seconds(), err))
				d := path.Join(resultsDir(), s.R.Name)
				_ = os.MkdirAll(d, 0o755)
				_ = os.WriteFile(path.Join(d, "restore-output.txt"), out, 0o644)
				restoreCh <- r
			}()
		}
		d, ok := s.WaitFor("R1 in RESTORE", 60*time.Second, func() bool { return s.tabletTypeHTTP(r1) == "RESTORE" })
		s.R.outcome("R1 reached RESTORE: %v (after %.1fs)", ok, d.Seconds())
		if !ok {
			s.R.violation("HARNESS: R1 never reached RESTORE")
		}
		if delay := os.Getenv("S12R_ERS_DELAY"); delay != "" {
			if dd, err := time.ParseDuration(delay); err == nil {
				s.Sleep(dd, "S12R_ERS_DELAY before ERS")
			}
		}

		// The ERS takes the shard lock while R1 restores.
		s.R.note("P semi-sync before ERS: %s", s.semiSync(p))
		s.cutPrimaryFromERSCaller(p)
		s.MarkFault()
		ers := s.startERS()

		var rr restoreResult
		select {
		case rr = <-restoreCh:
		case <-time.After(240 * time.Second):
			s.R.violation("HARNESS: restore did not finish in 240s")
		}
		doneAt := time.Now()
		s.R.outcome("restore finished %.1fs after ERS start (restore took %.1fs) err=%v result=%q", rr.end.Sub(s.Fault).Seconds(),
			rr.end.Sub(rr.start).Seconds(), rr.err, lastLines(rr.out, 3))
		// R1 right after the restore, before any repair.
		g, _ := r1.gtidExecuted()
		gp, _ := r1.scalar("select @@global.gtid_purged")
		rs, _ := r1.replicaStatus()
		io, src := "none", ""
		if rs != nil {
			io, src = rs["Replica_IO_Running"], rs["Source_Port"]
		}
		s.R.outcome("R1 right after restore: type=%s io=%s source_port=%s (P=%d) gtid_executed=%q gtid_purged=%q equals MANIFEST=%v",
			s.tabletTypeHTTP(r1), io, src, p.Tablet.MySQLPort, strings.ReplaceAll(g, "\n", ""), strings.ReplaceAll(gp, "\n", ""),
			strings.TrimPrefix(manifestPos, "MySQL56/") == strings.ReplaceAll(g, "\n", ""))
		s.R.note("P semi-sync at restore end: %s", s.semiSync(p))
		s.Sleep(2*time.Second, "after restore")
		s.R.note("P semi-sync 2s after restore end: %s", s.semiSync(p))
		s.relayState(r1, "R1-2s-after-restore")

		r, done := s.waitERS(ers, 240*time.Second)
		if !done {
			s.R.violation("HARNESS: ERS did not finish in 240s")
		} else {
			s.reportERS(r)
			s.R.outcome("ERS finished %.1fs after the restore", r.end.Sub(doneAt).Seconds())
		}
		s.R.outcome("topo primary right after ERS: %v; vttablet types: %s", aliasOf(s.topoPrimary()), s.types())
		s.R.note("R1 vttablet log: %s", strings.ReplaceAll(s.grepTabletLog(r1,
			"Restore:|leaving replication stopped|shard lock|initialize replication|StopReplicationAndGetStatus|CHANGE REPLICATION SOURCE|START REPLICA|RestoreFromBackup", 60), "\n", " || "))
		tagged := s.reportAckedSince("acked after ERS start", s.Fault, time.Now())

		// Heal and let VTOrc repair R1.
		s.Heal()
		for _, n := range s.Nodes {
			signalGroup(n.Group, syscall.SIGCONT)
		}
		s.Sleep(12*time.Second, "healed; VTOrc rediscovery")
		rs, _ = r1.replicaStatus()
		if rs != nil {
			io, src = rs["Replica_IO_Running"], rs["Source_Port"]
		} else {
			io, src = "none", ""
		}
		s.R.outcome("R1 before recoveries are re-enabled: type=%s io=%s source_port=%s topo primary=%s", s.tabletTypeHTTP(r1), io, src, aliasOf(s.topoPrimary()))
		s.setRecoveries(true)
		recoveriesOn := time.Now()
		d, ok = s.WaitFor("R1 replicating from the topo primary", 90*time.Second, func() bool {
			tp := s.topoPrimary()
			return tp != nil && s.replicatingFrom(r1, tp)
		})
		s.R.outcome("R1 replicating from topo primary %s after recoveries on: %v (after %.1fs)", aliasOf(s.topoPrimary()), ok, d.Seconds())
		s.R.note("VTOrc recoveries on R1 since %s: %s", recoveriesOn.Format("15:04:05.000"),
			strings.ReplaceAll(s.vtorcLogTail("zone2-0000000200.*(fixReplica|ReplicationStopped|NotConnectedToPrimary|Recovery for)|(fixReplica|ReplicationStopped|NotConnectedToPrimary).*zone2-0000000200", 15), "\n", " || "))
		s.Sleep(15*time.Second, "settle")
		s.reportErrantDetail("after settle")
		s.reportLostTagged("acked after ERS start (before invariant checks)", tagged)
	})
}

// vtorcLogTail returns the last max VTOrc log lines matching pattern.
func (s *Scenario) vtorcLogTail(pattern string, max int) string {
	out, _ := exec.Command("sh", "-c", fmt.Sprintf("grep -h -E %q %s/vtorc-*-stderr.txt 2>/dev/null | sort | cut -c1-300 | tail -n %d", pattern, s.CI.TmpDirectory, max)).CombinedOutput()
	return strings.TrimSpace(string(out))
}

func lastLines(s string, n int) string {
	l := strings.Split(strings.TrimSpace(s), "\n")
	if len(l) > n {
		l = l[len(l)-n:]
	}
	return strings.Join(l, " | ")
}

// S12r: restore through the RestoreFromBackup RPC; the ERS starts while it runs.
func TestS12rRestoreFinalizationDuringERS(t *testing.T) {
	s12r(t, "S12r-restore-finalization-during-ers", false)
}

// S12r2: as S12r, with the restore started by vttablet --restore-from-backup at startup.
func TestS12r2StartupRestoreFinalizationDuringERS(t *testing.T) {
	s12r(t, "S12r2-startup-restore-finalization-during-ers", true)
}

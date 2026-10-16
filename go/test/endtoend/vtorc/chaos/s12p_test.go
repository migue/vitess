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
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

// S12p and S12p2: local scenarios for the fourth rerun of the #21549 validation.

// S12p: S12a, with the new primary R2 unable to publish itself in the shard record. Just before
// ERS promotes R2 (when the harness releases the held appliers), R2's tablet group is blocked from
// the global etcd's client port, so its shard sync update fails and is retried after
// --shard-sync-retry-delay (30s). The block stays for 10s after ERS ends. Run with
// CHAOS_CELL_TOPO=per-cell, so that the cell topo (R2's tablet record) is on its own etcd and only
// the shard record update is blocked. A poller logs the shard record primary and R1's source.
func TestS12pLateShardPublication(t *testing.T) {
	runScenario(t, "S12p-late-shard-publication", Options{}, func(s *Scenario) {
		p := s.node("zone1-0000000100")
		r1 := s.node("zone2-0000000200")
		r2 := s.node("zone3-0000000300")
		s.ensurePrimary(p)
		s.setRecoveries(false)
		if s.Profile.SharedCellTopo {
			s.R.violation("HARNESS: S12p needs CHAOS_CELL_TOPO=per-cell (cell topos on the global etcd)")
		}
		globalPort := s.CI.TopoProcess.Port
		s.R.outcome("P=%s R1=%s (restarted) R2=%s (ERS choice, blocked from the global etcd port %d)", p.Tablet.Alias, r1.Tablet.Alias, r2.Tablet.Alias, globalPort)

		pollCtx, stopPoll := context.WithCancel(context.Background())
		defer stopPoll()
		var ersEndNs, pointedBack atomic.Int64
		go func() {
			last := ""
			for pollCtx.Err() == nil {
				ctx, cancel := context.WithTimeout(pollCtx, time.Second)
				si, err := s.Ts.GetShard(ctx, keyspaceName, shardName)
				cancel()
				prim := "?"
				if err == nil && si.PrimaryAlias != nil {
					prim = topoproto.TabletAliasString(si.PrimaryAlias)
				}
				src := "?"
				if rs, err := r1.replicaStatus(); err == nil && rs != nil {
					src = rs["Source_Port"] + "/" + rs["Replica_IO_Running"]
					if ersEndNs.Load() != 0 && rs["Source_Port"] == strconv.Itoa(p.Tablet.MySQLPort) {
						pointedBack.Add(1)
					}
				}
				cur := fmt.Sprintf("shard record primary=%s R1 source=%s (P port %d, R2 port %d)", prim, src, p.Tablet.MySQLPort, r2.Tablet.MySQLPort)
				if cur != last {
					s.Log.Add("poll", cur)
					last = cur
				}
				time.Sleep(200 * time.Millisecond)
			}
		}()

		s.Sleep(3*time.Second, "baseline")
		var held []*Node
		for _, n := range s.Nodes {
			if n != p && n != r1 {
				held = append(held, n)
			}
		}
		conns := make([]func(), 0, len(held))
		for _, n := range held {
			c := s.lockTable(n)
			conns = append(conns, func() {
				if _, err := c.ExecContext(t.Context(), "unlock tables"); err != nil {
					s.R.violation("HARNESS: unlock tables: %v", err)
				}
				_ = c.Close()
			})
		}
		if err := s.Net.BlockPorts(r1.Group, p.Tablet.MySQLPort); err != nil {
			t.Fatal(err)
		}
		s.Log.Add("fault", "cut R1 -> P mysql (R1 stops receiving)")
		s.Sleep(3*time.Second, "R1 behind; held tablets ack")
		s.cutPrimaryFromERSCaller(p)
		s.MarkFault()
		ers := s.startERS()
		_, ok := s.WaitFor("replica IO threads stopped by ERS", 30*time.Second, func() bool {
			for _, n := range s.Nodes {
				if n != p && s.ioRunning(n) != "No" {
					return false
				}
			}
			return true
		})
		if !ok {
			s.R.violation("HARNESS: ERS did not stop all replica IO threads")
		}
		if err := s.Net.Unblock(r1.Group, p.Tablet.MySQLPort); err != nil {
			t.Fatal(err)
		}
		s.Log.Add("heal", "R1 -> P mysql reachable again")
		release := s.Fault.Add(s12ApplierRelease)
		s.restartVttablet(r1)
		s.Sleep(max(time.Until(release), 0), "until the fixed applier release")

		// Block R2's tablet group from the global etcd just before ERS can promote R2.
		if err := s.Net.BlockPorts(r2.Group, globalPort); err != nil {
			t.Fatal(err)
		}
		s.Log.Add("fault", fmt.Sprintf("block %s -> global etcd port %d", r2.Tablet.Alias, globalPort))
		for _, f := range conns {
			f()
		}
		s.Log.Add("heal", "released applier locks")
		r, done := s.waitERS(ers, 240*time.Second)
		ersEnd := time.Now()
		ersEndNs.Store(ersEnd.UnixNano())
		if !done {
			s.R.violation("HARNESS: ERS did not finish in 240s")
		} else {
			s.reportERS(r)
		}
		s.R.outcome("topo primary right after ERS: %v; vttablet types: %s", aliasOf(s.topoPrimary()), s.types())
		s.Sleep(10*time.Second, "R2 still blocked from the global etcd after ERS")
		if err := s.Net.Unblock(r2.Group, globalPort); err != nil {
			t.Fatal(err)
		}
		s.Log.Add("heal", fmt.Sprintf("unblock %s -> global etcd", r2.Tablet.Alias))
		d, ok := s.WaitFor("shard record names R2", 90*time.Second, func() bool { return s.topoPrimary() == r2 })
		s.R.outcome("shard record names R2: %v, %.1fs after the unblock, %.1fs after ERS end", ok, d.Seconds(), time.Since(ersEnd).Seconds())
		s.R.outcome("R1 samples pointing at P after ERS end (poller, 200ms): %d", pointedBack.Load())
		s.R.note("R1 vttablet log: %s", s.grepTabletLog(r1, "initialize replication|Retrying deferred|Initialized deferred|rrant|SetReplicationSource|semi-sync setting", 30))
		tagged := s.reportAckedSince("acked after fault", s.Fault, time.Now())
		s.afterERS(nil)
		s.reportErrantDetail("after settle")
		s.reportLostTagged("acked after fault (before invariant checks)", tagged)
	})
}

// S12p2 (substitute for S12p, which this harness cannot express: a vttablet resolves every cell
// topo connection through the global topo's CellInfo, so blocking R2 from the global etcd also
// fails its tablet record update and PromoteReplica). S12a, where the harness makes the shard
// record name the old primary when ERS releases the lock: a watcher polls the record every 20ms
// during the ERS; as soon as R2 has published itself, R2's tablet group is blocked from the
// global etcd (so R2's shard sync cannot fix the record) and the record's primary is rewritten to
// P. ERS then releases the lock with the record naming P, the state a deferred R1 sees when the
// new primary publishes itself late. The block is lifted 10s after ERS ends.
func TestS12p2StaleShardRecordAtRelease(t *testing.T) {
	runScenario(t, "S12p2-stale-shard-record-at-release", Options{}, func(s *Scenario) {
		p := s.node("zone1-0000000100")
		r1 := s.node("zone2-0000000200")
		r2 := s.node("zone3-0000000300")
		s.ensurePrimary(p)
		s.setRecoveries(false)
		globalPort := s.CI.TopoProcess.Port
		s.R.outcome("P=%s R1=%s (restarted) R2=%s (ERS choice; record rewritten to P after it publishes itself)", p.Tablet.Alias, r1.Tablet.Alias, r2.Tablet.Alias)
		before, err := s.Ts.GetShard(t.Context(), keyspaceName, shardName)
		if err != nil {
			t.Fatal(err)
		}

		pollCtx, stopPoll := context.WithCancel(context.Background())
		defer stopPoll()
		var ersEndNs, pointedBack, rewroteNs atomic.Int64
		var watchFrom atomic.Bool
		go func() {
			last := ""
			for pollCtx.Err() == nil {
				ctx, cancel := context.WithTimeout(pollCtx, time.Second)
				si, err := s.Ts.GetShard(ctx, keyspaceName, shardName)
				cancel()
				prim := "?"
				if err == nil && si.PrimaryAlias != nil {
					prim = topoproto.TabletAliasString(si.PrimaryAlias)
				}
				if watchFrom.Load() && rewroteNs.Load() == 0 && prim == r2.Tablet.Alias {
					berr := s.Net.BlockPorts(r2.Group, globalPort)
					ctx, cancel := context.WithTimeout(pollCtx, 3*time.Second)
					_, uerr := s.Ts.UpdateShardFields(ctx, keyspaceName, shardName, func(si *topo.ShardInfo) error {
						si.PrimaryAlias = before.PrimaryAlias
						si.PrimaryTermStartTime = before.PrimaryTermStartTime
						return nil
					})
					cancel()
					rewroteNs.Store(time.Now().UnixNano())
					s.Log.Add("fault", fmt.Sprintf("R2 published itself; blocked %s -> global etcd port %d (err=%v) and rewrote the shard record primary to %s (err=%v)",
						r2.Tablet.Alias, globalPort, berr, p.Tablet.Alias, uerr))
					continue
				}
				src := "?"
				if rs, err := r1.replicaStatus(); err == nil && rs != nil {
					src = rs["Source_Port"] + "/" + rs["Replica_IO_Running"]
					if ersEndNs.Load() != 0 && rs["Source_Port"] == strconv.Itoa(p.Tablet.MySQLPort) {
						pointedBack.Add(1)
					}
				}
				cur := fmt.Sprintf("shard record primary=%s R1 source=%s (P port %d, R2 port %d)", prim, src, p.Tablet.MySQLPort, r2.Tablet.MySQLPort)
				if cur != last {
					s.Log.Add("poll", cur)
					last = cur
				}
				time.Sleep(20 * time.Millisecond)
			}
		}()

		s.Sleep(3*time.Second, "baseline")
		var held []*Node
		for _, n := range s.Nodes {
			if n != p && n != r1 {
				held = append(held, n)
			}
		}
		var unlocks []func()
		for _, n := range held {
			c := s.lockTable(n)
			unlocks = append(unlocks, func() {
				if _, err := c.ExecContext(t.Context(), "unlock tables"); err != nil {
					s.R.violation("HARNESS: unlock tables: %v", err)
				}
				_ = c.Close()
			})
		}
		if err := s.Net.BlockPorts(r1.Group, p.Tablet.MySQLPort); err != nil {
			t.Fatal(err)
		}
		s.Log.Add("fault", "cut R1 -> P mysql (R1 stops receiving)")
		s.Sleep(3*time.Second, "R1 behind; held tablets ack")
		s.cutPrimaryFromERSCaller(p)
		s.MarkFault()
		ers := s.startERS()
		_, ok := s.WaitFor("replica IO threads stopped by ERS", 30*time.Second, func() bool {
			for _, n := range s.Nodes {
				if n != p && s.ioRunning(n) != "No" {
					return false
				}
			}
			return true
		})
		if !ok {
			s.R.violation("HARNESS: ERS did not stop all replica IO threads")
		}
		if err := s.Net.Unblock(r1.Group, p.Tablet.MySQLPort); err != nil {
			t.Fatal(err)
		}
		s.Log.Add("heal", "R1 -> P mysql reachable again")
		release := s.Fault.Add(s12ApplierRelease)
		s.restartVttablet(r1)
		s.Sleep(max(time.Until(release), 0), "until the fixed applier release")
		watchFrom.Store(true)
		for _, f := range unlocks {
			f()
		}
		s.Log.Add("heal", "released applier locks")
		r, done := s.waitERS(ers, 240*time.Second)
		ersEnd := time.Now()
		ersEndNs.Store(ersEnd.UnixNano())
		if !done {
			s.R.violation("HARNESS: ERS did not finish in 240s")
		} else {
			s.reportERS(r)
		}
		if rw := rewroteNs.Load(); rw != 0 {
			s.R.outcome("shard record rewritten to P %.3fs before ERS end", ersEnd.Sub(time.Unix(0, rw)).Seconds())
		} else {
			s.R.violation("HARNESS: the shard record never named R2 during the ERS; no rewrite")
		}
		s.R.outcome("topo primary right after ERS: %v; vttablet types: %s", aliasOf(s.topoPrimary()), s.types())
		s.Sleep(10*time.Second, "R2 blocked from the global etcd after ERS")
		if err := s.Net.Unblock(r2.Group, globalPort); err != nil {
			t.Fatal(err)
		}
		s.Log.Add("heal", fmt.Sprintf("unblock %s -> global etcd", r2.Tablet.Alias))
		d, ok := s.WaitFor("shard record names R2", 60*time.Second, func() bool { return s.topoPrimary() == r2 })
		s.R.outcome("shard record names R2 again: %v, %.1fs after the unblock", ok, d.Seconds())
		if !ok {
			_, err := s.Ts.UpdateShardFields(t.Context(), keyspaceName, shardName, func(si *topo.ShardInfo) error {
				ti, err := s.Ts.GetTablet(t.Context(), r2.Tablet.GetAlias())
				if err != nil {
					return err
				}
				si.PrimaryAlias = r2.Tablet.GetAlias()
				si.PrimaryTermStartTime = ti.PrimaryTermStartTime
				return nil
			})
			s.Log.Add("heal", fmt.Sprintf("harness wrote R2 into the shard record err=%v", err))
		}
		s.R.outcome("R1 samples pointing at P after ERS end (poller, 20ms): %d", pointedBack.Load())
		s.R.note("R1 vttablet log: %s", s.grepTabletLog(r1, "initialize replication|Retrying deferred|Initialized deferred|rrant|SetReplicationSource|semi-sync setting", 30))
		tagged := s.reportAckedSince("acked after fault", s.Fault, time.Now())
		s.afterERS(nil)
		s.reportErrantDetail("after settle")
		s.reportLostTagged("acked after fault (before invariant checks)", tagged)
	})
}

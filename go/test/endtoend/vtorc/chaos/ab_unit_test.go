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
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"
)

func TestRestartPolicyDelay(t *testing.T) {
	k8s := restartPolicies["k8s"]
	for exits, want := range map[int]time.Duration{1: 10 * time.Second, 2: 20 * time.Second, 5: 160 * time.Second, 6: 5 * time.Minute, 20: 5 * time.Minute} {
		assert.Equal(t, want, k8s.delay(exits), "exits=%d", exits)
	}
	assert.Equal(t, time.Second, restartPolicies["fast"].delay(7))
}

func TestPairLockLines(t *testing.T) {
	at := func(s string) time.Time {
		tm, err := time.ParseInLocation("15:04:05", s, time.Local)
		require.NoError(t, err)
		return tm
	}
	lines := []logLine{
		{T: at("10:00:00"), File: "vtctld-stderr.txt", Text: "Locking shard ks/0 for action EmergencyReparentShard(zone2-0000000400) with options: {}"},
		{T: at("10:00:01"), File: "vtorc-zone1-stderr.txt", Text: "Locking shard ks/0 for action VTOrc Recovery for PrimarySemiSyncBlocked on zone1-0000000100 with options: {}"},
		{T: at("10:00:05"), File: "vtctld-stderr.txt", Text: "Unlocking shard ks/0 for action EmergencyReparentShard(zone2-0000000400) with error could not reach sufficient tablets"},
		{T: at("10:00:20"), File: "vtctld-stderr.txt", Text: "Locking shard ks/0 for action EmergencyReparentShard(zone2-0000000400) with options: {}"},
		{T: at("10:00:25"), File: "vtctld-stderr.txt", Text: "Unlocking shard ks/0 for successful action EmergencyReparentShard(zone2-0000000400)"},
		{T: at("10:00:30"), File: "vtctld-stderr.txt", Text: "Locking shard ks/0 for action PlannedReparentShard(zone2-0000000400, AvoidPrimary = <nil>) with options: {}"},
	}
	ers := pairLockLines(lines, ersAction)
	require.Len(t, ers, 2)
	assert.False(t, ers[0].OK)
	assert.Equal(t, "could not reach sufficient tablets", ers[0].Err)
	assert.Equal(t, at("10:00:05"), ers[0].Unlock)
	assert.True(t, ers[1].OK)

	orc := pairLockLines(lines, vtorcAction)
	require.Len(t, orc, 1)
	assert.Equal(t, "vtorc-zone1", orc[0].Who)
	assert.True(t, orc[0].Unlock.IsZero(), "still holding the lock")
	require.Len(t, pairLockLines(lines, prsAction), 1)
}

func TestLogTime(t *testing.T) {
	tm, ok := logTime("\x1b[2m2026-10-17 00:28:29.395 CEST\x1b[0m INF topo/locks.go:167 Locking shard ks/0")
	require.True(t, ok)
	assert.Equal(t, 395*time.Millisecond, time.Duration(tm.Nanosecond()))
}

func TestPromoteReplicaRe(t *testing.T) {
	assert.True(t, promoteReplicaRe.MatchString("2026-10-17 00:15:06.775 UTC INF tabletmanager/rpc_replication.go:1247 PromoteReplica"))
	assert.True(t, promoteReplicaRe.MatchString("\x1b[2m2026-10-17 00:15:06.775 CEST\x1b[0m \x1b[92mINF\x1b[0m \x1b[2mtabletmanager/rpc_replication.go:1247\x1b[0m PromoteReplica"))
	assert.False(t, promoteReplicaRe.MatchString("2026-10-17 00:15:06.776 UTC INF tabletmanager/rpc_server.go:83 TabletManager.PromoteReplica()(on zone2-0000000400 from ): ok"))
}

// TestSupervisorRestartsExitedGenerations runs a "vttablet" that exits at once: every
// generation must be restarted after the policy's delay, with its own log, the same command
// line, and classified as exiting by itself.
func TestSupervisorRestartsExitedGenerations(t *testing.T) {
	t.Setenv("CHAOS_SUPERVISOR", "fast")
	t.Setenv("CHAOS_RESULTS_DIR", t.TempDir())
	dir := t.TempDir()
	bin := path.Join(dir, "vttablet")
	require.NoError(t, os.WriteFile(bin, []byte("#!/bin/sh\necho \"E1017 started $$\" >&2\nexit 1\n"), 0o755))
	vp := &cluster.VttabletProcess{Binary: bin, LogDir: dir, TabletPath: "zone2-0000000200", ErrorLog: path.Join(dir, "gen0.txt")}
	n := &Node{Tablet: &cluster.Vttablet{Alias: "zone2-0000000200", VttabletProcess: vp}}
	s := &Scenario{Chaos: &Chaos{t: t, Log: NewEventLog("supervisor-unit")}, R: &Report{Name: "supervisor-unit"}}

	prepared := false
	sv := s.Supervise(n, false, func() { prepared = true })
	assert.True(t, prepared)
	require.Eventually(t, func() bool { return sv.SelfExits() >= 3 }, 30*time.Second, 50*time.Millisecond)
	sv.Stop()

	gens := sv.Generations()
	assert.True(t, gens[0].Induced)
	logs := map[string]bool{}
	for _, g := range gens[1:] {
		assert.False(t, g.Induced)
		assert.Equal(t, gens[1].ArgvSHA, g.ArgvSHA)
		assert.Contains(t, g.LastLine, "E1017 started")
		logs[g.Log] = true
		if g.N > 1 {
			assert.Equal(t, time.Second, g.Delay)
		}
	}
	assert.Len(t, logs, len(gens)-1, "each generation has its own log")
	assert.Empty(t, s.R.Violations)
	sv.Check()
	assert.Empty(t, s.R.Violations)
	_, err := os.Stat(path.Join(os.Getenv("CHAOS_RESULTS_DIR"), "supervisor-unit", "supervisor-zone2-0000000200.json"))
	assert.NoError(t, err)
}

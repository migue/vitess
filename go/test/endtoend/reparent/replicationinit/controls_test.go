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

// Package replicationinit holds end-to-end tests of replication initialization at vttablet
// startup and its interaction with reparents. The tests in this file are controls: they show
// that the fixture's restart and shard lock behave as the scenarios rely on, and pass on main.
package replicationinit

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/test/endtoend/reparent/utils"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
)

// teardownOnCleanup stops the cluster when the test ends. A failed test keeps its VTDATAROOT
// and writes the tails of the component logs to the test output, which CI retains.
func teardownOnCleanup(t *testing.T, ci *cluster.LocalProcessCluster) {
	t.Cleanup(func() {
		if !t.Failed() {
			utils.TeardownCluster(ci)
			return
		}
		logs, _ := filepath.Glob(path.Join(ci.TmpDirectory, "*stderr.txt*"))
		for _, l := range logs {
			b, _ := os.ReadFile(l)
			lines := strings.Split(string(b), "\n")
			t.Logf("==== last lines of %s\n%s", l, strings.Join(lines[max(0, len(lines)-40):], "\n"))
		}
		ci.Teardown()
		t.Logf("kept %s for the failed test", ci.CurrentVTDATAROOT)
	})
}

// restartVttablet restarts only tab's vttablet: its mysqld and replication state stay. The
// log of the stopped process is kept as <log>.gen<gen>.
func restartVttablet(t *testing.T, tab *cluster.Vttablet, gen int) {
	vp := tab.VttabletProcess
	require.NoError(t, vp.TearDownWithTimeout(30*time.Second))
	require.NoError(t, os.Rename(vp.ErrorLog, fmt.Sprintf("%s.gen%d", vp.ErrorLog, gen)))
	// The caller waits for the state it expects.
	vp.ServingStatus = ""
	require.NoError(t, vp.Setup())
}

func replicaStatus(t *testing.T, tab *cluster.Vttablet) map[string]string {
	qr := utils.RunSQL(t.Context(), t, "show replica status", tab)
	require.Len(t, qr.Rows, 1)
	st := map[string]string{}
	for i, f := range qr.Fields {
		st[f.Name] = qr.Rows[0][i].ToString()
	}
	return st
}

func mysqldPid(t *testing.T, tab *cluster.Vttablet) string {
	b, err := os.ReadFile(path.Join(tab.MysqlctlProcess.BasePath(), "mysql.pid"))
	require.NoError(t, err)
	return strings.TrimSpace(string(b))
}

// TestRestartReplicaVttabletKeepsMysqld restarts a replica's vttablet without its mysqld.
// The new process answers tablet manager RPCs, replicates from the primary, serves, and
// replicates a later write.
func TestRestartReplicaVttabletKeepsMysqld(t *testing.T) {
	ci := utils.SetupReparentCluster(t, policy.DurabilitySemiSync)
	teardownOnCleanup(t, ci)
	tablets := ci.Keyspaces[0].Shards[0].Vttablets
	primary, replica := tablets[0], tablets[1]

	before := utils.ConfirmReplication(t, primary, tablets[1:])
	pid := mysqldPid(t, replica)
	gen0Log := replica.VttabletProcess.ErrorLog

	restartVttablet(t, replica, 0)

	require.Equal(t, pid, mysqldPid(t, replica), "mysqld was restarted")
	require.FileExists(t, gen0Log+".gen0")
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		out, err := ci.VtctldClientProcess.ExecuteCommandWithOutput("GetFullStatus", replica.Alias)
		assert.NoError(c, err, out)
	}, 30*time.Second, 100*time.Millisecond, "the restarted vttablet does not answer tablet manager RPCs")
	utils.WaitForTabletToBeServing(t.Context(), t, ci, replica, 30*time.Second)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		st := replicaStatus(t, replica)
		assert.Equal(c, "Yes", st["Replica_IO_Running"])
		assert.Equal(c, "Yes", st["Replica_SQL_Running"])
		assert.Equal(c, strconv.Itoa(primary.MySQLPort), st["Source_Port"])
	}, 30*time.Second, 100*time.Millisecond)
	require.NoError(t, utils.CheckInsertedValues(t.Context(), t, replica, before))
	utils.ConfirmReplication(t, primary, tablets[1:])
}

// heldLock is a test-owned shard lock. Its etcd lease is renewed only when the lock is
// checked, so a checker calls topo.CheckShardLocked with the lock's context every interval;
// the first failed check ends the checker and is its result.
type heldLock struct {
	ctx      context.Context
	unlock   func(*error)
	stop     context.CancelFunc
	checkErr chan error
}

func holdShardLock(t *testing.T, ts *topo.Server, action string, ttl, interval time.Duration) *heldLock {
	lockCtx, unlock, err := ts.LockShard(t.Context(), utils.KeyspaceName, utils.ShardName, action, topo.WithTTL(ttl))
	require.NoError(t, err)
	stopCtx, stop := context.WithCancel(t.Context())
	l := &heldLock{ctx: lockCtx, unlock: unlock, stop: stop, checkErr: make(chan error, 1)}
	go func() {
		for {
			select {
			case <-stopCtx.Done():
				l.checkErr <- nil
				return
			case <-time.After(interval):
			}
			if err := topo.CheckShardLocked(lockCtx, utils.KeyspaceName, utils.ShardName); err != nil {
				l.checkErr <- err
				return
			}
		}
	}()
	t.Cleanup(func() {
		// Releases a lock the test did not release: an unlock error is expected after a failure.
		stop()
		var uerr error
		unlock(&uerr)
	})
	return l
}

// release stops the checker, checks ownership one last time and unlocks.
func (l *heldLock) release(t *testing.T) {
	l.stop()
	require.NoError(t, <-l.checkErr, "the lock was lost while held")
	require.NoError(t, topo.CheckShardLocked(l.ctx, utils.KeyspaceName, utils.ShardName))
	var err error
	l.unlock(&err)
	l.unlock = func(*error) {}
	require.NoError(t, err)
}

func tryLockShard(t *testing.T, ts *topo.Server) error {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, unlock, err := ts.TryLockShard(ctx, utils.KeyspaceName, utils.ShardName, "contender")
	if err == nil {
		var uerr error
		unlock(&uerr)
		require.NoError(t, uerr)
	}
	return err
}

// TestShardLockControl holds a test-owned shard lock past its lease TTL with periodic checks.
// A non-blocking lock attempt fails while it is held and succeeds after release, and the
// checker reports a lease that etcd revoked.
func TestShardLockControl(t *testing.T) {
	ci := cluster.NewCluster("zone1", "localhost")
	teardownOnCleanup(t, ci)
	require.NoError(t, ci.StartTopo())
	require.Equal(t, "etcd2", *ci.TopoFlavorString(), "the lease checks below are etcd specific")
	ts := ci.TopoProcess.Server
	require.NoError(t, ts.CreateKeyspace(t.Context(), utils.KeyspaceName, &topodatapb.Keyspace{}))
	require.NoError(t, ts.CreateShard(t.Context(), utils.KeyspaceName, utils.ShardName))
	const ttl, interval = 3 * time.Second, time.Second

	l := holdShardLock(t, ts, "control hold", ttl, interval)
	err := tryLockShard(t, ts)
	require.True(t, topo.IsErrType(err, topo.NodeExists), "a second lock attempt while held: %v", err)
	require.Never(t, func() bool { return len(l.checkErr) > 0 }, 2*ttl, 100*time.Millisecond, "the checker failed while the lock was held")
	l.release(t)
	require.NoError(t, tryLockShard(t, ts), "the lock was not released")

	// A lost lease: revoke the lock's lease in etcd and expect the checker to fail.
	l = holdShardLock(t, ts, "control lost lease", ttl, interval)
	cli, err := clientv3.New(clientv3.Config{Endpoints: []string{net.JoinHostPort(ci.TopoProcess.Host, strconv.Itoa(ci.TopoProcess.Port))}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer cli.Close()
	locks := path.Join(cluster.TopoGlobalRoot("etcd2"), "keyspaces", utils.KeyspaceName, "shards", utils.ShardName, "locks")
	resp, err := cli.Get(t.Context(), locks, clientv3.WithPrefix())
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	_, err = cli.Revoke(t.Context(), clientv3.LeaseID(resp.Kvs[0].Lease))
	require.NoError(t, err)
	select {
	case err := <-l.checkErr:
		require.Error(t, err)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "the checker did not report the revoked lease")
	}
}

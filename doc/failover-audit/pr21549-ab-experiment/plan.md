# #21549: A versus B, second pilot

## Instructions for the executing agent

This is a comparison, not a validation of the PR. It answers the reviewer's question (maxenglander, 16 October): when replicas restart while a shard operation holds the lock, does design B (a starting vttablet exits instead of deferring) make an ERS much slower, or fail to converge, compared with design A (the PR: it defers, stays out of rotation, and answers RPCs)?

Report what happened. Do not tune either design to make it pass. If a scenario cannot be run as written, say so, describe what was run instead, and do not count the run.

- Do not publish, push, comment or open anything on GitHub, and do not modify the PR. Deliver the results as a downloadable bundle (below).
- This directory is on the harness branch `chaos/pr21549-ab-experiment`. Everything the run needs is here or pinned below.
- Keep any harness fix as a separate local commit on top of this branch, with a one-line reason, and include it as a patch in the bundle.

## Why a second pilot

The first pilot (E1 and E3, sup-fast, 4 runs per design) did not support a comparison:

1. **The barrier held the ERS in only 4 of 8 E1 runs.** The harness cut every replica from P at once, so the replicas' received positions tied or differed by chance. The ERS leaves out of its relay-log wait only candidates strictly behind the most advanced one; when R1 or R2 was not strictly behind, it finished applying first, the ERS made it an intermediate source, and the repoint of R3 or R4 hung behind the barrier.
2. **Every long ERS lost its shard lock after about 30 s, on both designs.** `topo.Server`'s lock functions acquire under a context they cancel on return, and etcd2topo ran the lease's KeepAlive on that context, so the KeepAlive stopped as soon as the lock was taken. The lease then lived only `--topo-etcd-lease-ttl` (30 s) past the holder's last `CheckShardLocked`. This bug predates the PR.
3. **Under B, the ERS stalled on the restarted tablets.** `gatherReparentJournalInfo` needs `ReadReparentJournalInfo` from every valid candidate, including R1 and R2, which answered `StopReplicationAndGetStatus` before they restarted. Under B they cannot answer until a generation takes the shard lock, which the ERS holds. In both valid B runs, the calls were answered only after the lease expired (finding 2) and a generation took the lock; the ERS then failed with `lost topology lock`, and a retry 10 s later succeeded. Under A the deferring tablets answered within 0.2 s and the first ERS succeeded in 1.4 to 1.7 s.

The question for this pilot: **with the lock held for the whole ERS, what does B's first ERS do?** The expectation from the code: the journal collection fails at the ERS's `--wait-replicas-timeout` (120 s) with `could not read reparent journal information`. The ERS then runs its failure cleanup (`restartReplicationOnStoppedReplicas`) while it still holds the lock; the cleanup also contacts R1 and R2 and has its own `RemoteOperationTimeout` (15 s by default). Only after the unlock can R1 and R2 start, initializing from P. An unlock about 135 s after the lock is therefore consistent with the expectation. Measure it; do not assume it.

What changed, so that both designs run under the same conditions:
- **Lease fix** (`patches/lease`, applied to both designs): the KeepAlive runs until the unlock. It is a labelled condition of this pilot. The fix is not part of #21549; it is proposed separately.
- **Deterministic barrier** (harness): R1 and R2 are cut from P first; R3 and R4 are cut only once they have received strictly more. The harness checks that the ERS waits on R3's or R4's relay logs, or both (their `WaitForPosition`), and on neither R1's nor R2's, and that R1's and R2's receivers were revoked within 20 s. Otherwise it reports `FIXTURE INVALID` and does not restart anything.
- **Convergence includes writes and the new primary's serving state** (harness): see Measurements.
- **Phase (d) starts at the promotion** (harness): the new primary's `PromoteReplica` in the first successful attempt, not the attempt's unlock.
- **The first pilot's eight reporting fixes** are on this branch.

## Pinned inputs

| Role | Revision | Notes |
|---|---|---|
| Experiment base | `1463c6447044d10ccd024bf1521d10be49c262ad` | The head of `vitessio/vitess#21549` on 17 October; the PR has moved on since. Fetch this revision by SHA, not the PR's current head: `git fetch https://github.com/vitessio/vitess 1463c6447044d10ccd024bf1521d10be49c262ad && git checkout --detach FETCH_HEAD`. Apply the patches to this revision only. |
| **A** | Experiment base + `patches/a/*.patch` + `patches/lease/*.patch` (`git am`, in that order) | The PR's eight local commits (A's head before the lease patch: `tabletmanager: keep replication stopped after ResetReplicationParameters`, original SHA `359b377b85`), then the lease fix. |
| **B** | A + `patches/b/*.patch` | A prototype (original SHA `0244696de1`): where A defers, B returns the error. At startup, vttablet then exits. A's deferral code remains but is never entered. |
| Harness | this branch | The first pilot's harness (`29d0cdecd0`), its eight reporting fixes, and four commits: `chaos: hold the A/B ERS at R3's and R4's relay-log wait deterministically`, `chaos: require an acknowledged write for A/B convergence, then keep writing for 10s`, `chaos: require the new primary to serve for A/B convergence, as the replicas must`, `chaos: start A/B phase (d) at the new primary's PromoteReplica, not at the reparent's unlock`, and, added during the second pilot, `chaos: read each replica's gtid_executed before the primary's in the errant GTID report` (it changes only that report line; the second pilot's r01–r03 ran without it, r04–r08 with it). Directory `go/test/endtoend/vtorc/chaos`. |

Before building, verify the patches: `cd doc/failover-audit/pr21549-ab-experiment/patches && sha256sum -c checksums.sha256`.

**Verify the lease fix in both trees** before building the cluster binaries: in the A and the B checkout, run `go test ./go/vt/topo/etcd2topo -run '^TestEtcd2Topo$' -count=1` with etcd 3.5.17 on `PATH`. It must pass. (Its last case holds a keyspace lock through `topo.Server` with a 1 s lease and checks, without renewing it, that the lock survives 5 s; it fails without the fix.) Record the output.

Build the eight cluster binaries (`vttablet vtctld vtctldclient vtgate vtorc mysqlctl mysqlctld vtctl`) from separate A and B checkouts, each with `GOTOOLCHAIN=go1.27.1 CGO_ENABLED=0 go build -trimpath -o <dir>/<bin> ./go/cmd/<bin>`. Build the chaos test binary once, from this branch. Record sha256 checksums of every artifact. Before each run, copy the revision's binaries into this checkout's `bin/` and verify them with `sha256sum -c`. Record each run's binary source SHA.

## Environment

As in the first pilot:

| | Requirement |
|---|---|
| OS | Ubuntu 24.04, linux/amd64, 4+ vCPU |
| cgroup | v2, unified or hybrid; `chaos_run.sh` auto-detects it |
| iptables | `cgroup` and `connmark` matches working |
| Go | 1.27.1 (`GOTOOLCHAIN=go1.27.1`) |
| MySQL | 8.4.6. The generic tarball is preferred. If it is unavailable, extract it from the `mysql:8.4.6` Docker image and record the digests. |
| etcd | 3.5.17. Not ≥ 3.7: its `TryLock` cannot detect a held lock (#21548). |
| User | root runner, dropping to `RUN_USER=ubuntu` |

Known pitfalls:
- `sudo` drops `PATH`. Pass it with `sudo env PATH="$PATH" ...`, otherwise `chaos_run.sh` fails silently while sourcing `build.env`.
- The MySQL socket path is limited to 107 bytes. Use a short VTDATAROOT such as `/work/vd/rNN`, mapped to run IDs in `vtdata-map.txt`.
- Containers whose PID 1 reaps orphans slowly break the harness's `kill -9` and restart sequences. Run every attempt under a child subreaper (`prctl(PR_SET_CHILD_SUBREAPER)` plus a `wait4(-1)` loop), uniformly for every revision. The first pilot's `tools/main.go` does this.
- Set `CHAOS_KEEP_DATA=1` to keep the runtime directories for evidence.

## Supervisor

B relies on something restarting vttablet when it exits. The harness supervises each restarted vttablet itself (`Supervisor` in `supervisor.go`), with the same arguments and environment for every generation, its own exit channel and log per generation, restore flags off in E1 and E4, and each exit recorded as induced (generation 0 only), startup failure, or teardown. This pilot runs **sup-fast** only: restart delay 1 s, fixed. Keep the supervisor enabled for A as well.

## Fixture

Profile `semisync-3vtorc` (semi-sync durability). Five tablets: P = zone1-100 (primary), R1 = zone2-200 and R2 = zone3-300 (restarted), R3 = zone2-400 (the ERS target), R4 = zone3-500 (an acker for R3). Writers running throughout, with acknowledged-write accounting. VTOrc recoveries disabled until the reparent phase ends.

### The barrier

1. A harness session on R3 and on R4 holds `LOCK TABLES vt_ks.chaos_t READ`, so their SQL threads stop applying while their receivers keep receiving.
2. R1 and R2 are cut from P's MySQL port. Writes continue, acknowledged by R3 and R4.
3. Once R3 and R4 have each received strictly more GTIDs than R1 and R2 (at most 15 s), R3 and R4 are cut too, and P's commits block on semi-sync.
4. The ERS (`--wait-replicas-timeout 120s`) then excludes R1 and R2 from its relay-log wait and waits on R3 and R4 (or only the one ahead, if they differ), holding the shard lock.

The harness checks steps 3 and 4 and reports `FIXTURE INVALID` if either fails. Such a run is not counted. If more than one run in four is invalid, stop and report instead of continuing.

## Scenarios

Tests in `ab_test.go`. Run each with its binaries in `bin/` and a results directory per design, for example:

```
CHAOS_PROFILE=semisync-3vtorc CHAOS_SUPERVISOR=fast CHAOS_RESULTS_DIR=/home/ubuntu/chaos-results/ab2-A-fast \
  go/test/endtoend/vtorc/chaos/chaos_matrix.sh TestABE4ControlERS
```

Order, interleaving A and B: E4 once per design, then E1 three times per design (A, B, A, B, A, B).

### E4: control

E1 without restarts or barrier: the baseline ERS duration, and a check that the staggered cut alone does not change the ERS.

### E1: replicas restart during a manual ERS against a reachable primary

1. Set the barrier and block P as above.
2. Run `EmergencyReparentShard` to R3. Wait for the revocation on R1 and R2, and for the ERS to wait on R3 or R4 and on neither R1 nor R2.
3. Make P's MySQL port reachable from R1 and R2 again. Restart R1's and R2's vttablets (mysqld untouched). Release the barrier once both have reached their initialization outcome.
4. If the ERS fails, rerun it (without the barrier, `--wait-replicas-timeout 20s`) every 10 s until it succeeds, for up to 10 min from step 2.

**Stop rule:** if any run's vtctld or tablet logs show a lost lease (`node doesn't exist: lease`, `lost topology lock`), stop and investigate whether the lease fix is present in that run's binaries and whether it is sufficient. Report what you find before running further.

## Measurements, per run

The harness writes these into each scenario's `report.txt`, with `ab-timeline.json`, `repl-samples.tsv`, `reparent-output.txt` and the supervisor records.

- **Reparent:** each attempt's lock acquisition, unlock, outcome and error; time from the first attempt to the first success.
- **The ERS's journal stage (E1)**, reconstructed from the tablets' logs and vtctld: when the ERS sent `ReadReparentJournalInfo` to each tablet, when each answered (or whether it never did), the ERS's error text, and whether the shard lock was held by the ERS throughout (no other holder's `Locking`/`Unlocking shard` lines in between). Record separately, each with its timestamp: the journal collection's failure (if any), the end of the ERS's failure cleanup (its `StartReplication` calls to the tablets, and their outcome), and the unlock. For B, state which of the following happened: the journal collection failed at its `--wait-replicas-timeout`, followed by the cleanup and the unlock; the ERS failed earlier or for another reason; it succeeded.
- **After a failed ERS:** what R1 and R2 did once the lock was free (initialized from which source), whether P acknowledged writes again before a successful reparent, and when.
- **Restarts:** per tablet, the generations with start, exit, classification and error.
- **Convergence:** the time from the first reparent attempt until (1) the shard record names a new primary whose vttablet is `PRIMARY` and `SERVING`, every other replica except the old primary serves as a `REPLICA` replicating from it, and (2) a write that started after (1) held has been acknowledged. The harness reports `CONVERGED` at that acknowledgement and then keeps the writers running for 10 s before its analysis, so that `WRITES` covers a completed window. The old primary is left out: its blocked commits are errant once R3 is promoted, on either design. Not converged 10 min after the first attempt: **censored**.
- **Writes:** acknowledged-write loss (must be zero); the longest gap between acknowledged writes; when writes were acknowledged again after the first successful reparent.
- **Connections to P, by phase:** (a) existing before the revocation; (b) a receiver connecting to P after its revocation while the reparent holds the lock; (c) after a failed reparent; (d) after a successful promotion of another tablet, from that tablet's `PromoteReplica` (the harness prints it as `promotion at`). Report (b) and (d) per run, with the tablet, its generation, and what configured the connection.
- **Other safety:** any errant GTID other than P's; any replica serving while not replicating.

## Deliverables

A downloadable `.tar.gz` bundle, as for the first pilot: `INDEX.md`, `BUILD.md` (with the etcd2topo test output from both trees), `environment.md`, the binaries' checksums and source SHAs, the harness fixes as patches, the run table with the required timestamps, the raw logs, the per-generation supervisor records, and a `SUMMARY.md` that states:
- for E4 and E1, A versus B: reparent success of the first attempt, time to the first success, convergence time, with each run's values;
- for B's E1 runs, the journal stage as described above (journal failure, cleanup end and unlock, each timed), and whether the expectation held;
- startup failures and induced restarts per design;
- connections to P in phases (b) and (d), on either design, and when P acknowledged writes again;
- runs excluded, with the reason.

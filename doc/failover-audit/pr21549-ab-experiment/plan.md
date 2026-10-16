# #21549: A versus B experiment

## Instructions for the executing agent

This is a comparison, not a validation of the PR. It answers the reviewer's question (maxenglander, 16 October): when replicas restart while a shard operation holds the lock, does design B (a starting vttablet exits instead of deferring) make an ERS much slower, or fail to converge, compared with design A (the PR: it defers, stays out of rotation, and answers RPCs)?

Report what happened. Do not tune either design to make it pass. If a scenario cannot be run as written, say so, describe what was run instead, and do not count the run.

- Do not publish, push, comment or open anything on GitHub, and do not modify the PR. Deliver the results as a downloadable bundle (below).
- This directory is on the harness branch `chaos/pr21549-ab-experiment`. Everything the run needs is here or pinned below.
- Keep any harness fix as a separate local commit on top of this branch, with a one-line reason, and include it as a patch in the bundle.

## Pinned inputs

| Role | Revision | Notes |
|---|---|---|
| PR base | `1463c6447044d10ccd024bf1521d10be49c262ad` | `refs/pull/21549/head` of `vitessio/vitess` on 17 October. Fetch it with `git fetch https://github.com/vitessio/vitess pull/21549/head`. If it is anything else, stop and ask. |
| **A** | PR base + `patches/a/*.patch` (`git am`, in order) | Eight commits, not pushed anywhere. Expected head subject: `tabletmanager: keep replication stopped after ResetReplicationParameters` (original SHA `359b377b85`). |
| **B** | A + `patches/b/*.patch` | A prototype (original SHA `0244696de1`): where A defers, B returns the error. At startup, vttablet then exits. A restore over RPC fails and leaves the tablet `RESTORE`. A vttablet restart that skips `--restore-from-backup` because mysqld already has data initializes replication as an ordinary start does. A's deferral code remains but is never entered. |
| main | `2b77bd16d671f04d69e585da064cdd7040264d15` | Reference for E1 only: no shard lock at startup. Record what it does, including any reconnection to the old primary. |
| Harness | this branch | `a9c7a464` (upstream branch `claude/practical-dijkstra-ucvu74`) + the rerun patches for S12r, H1, H2, S12h, S12p and S12f + the experiment commit (`chaos: add the #21549 A/B experiment ...`). Directory `go/test/endtoend/vtorc/chaos`. |

Before building, verify the patches: `cd doc/failover-audit/pr21549-ab-experiment/patches && sha256sum -c checksums.sha256`.

The experiment commit compiles for linux/amd64 and its unit tests pass (`go test ./go/test/endtoend/vtorc/chaos/` without `CHAOS_E2E`): the supervisor restarting a process that exits at once, the restart policies, the pairing of lock lines, and log timestamps. It has **never run end to end**; the pilot is its first run. Fix what the pilot shows, and record each fix.

Build the eight cluster binaries (`vttablet vtctld vtctldclient vtgate vtorc mysqlctl mysqlctld vtctl`) from separate A, B and main checkouts, each with `GOTOOLCHAIN=go1.27.1 CGO_ENABLED=0 go build -trimpath -o <dir>/<bin> ./go/cmd/<bin>`. Build the chaos test binary once, from this branch. Record sha256 checksums of every artifact. Before each run, copy the revision's binaries into this checkout's `bin/` and verify them with `sha256sum -c`. Record each run's binary source SHA.

## Environment

| | Requirement |
|---|---|
| OS | Ubuntu 24.04, linux/amd64, 4+ vCPU |
| cgroup | v2, unified or hybrid; `chaos_run.sh` auto-detects it |
| iptables | `cgroup` and `connmark` matches working |
| Go | 1.27.1 (`GOTOOLCHAIN=go1.27.1`) |
| MySQL | 8.4.6. The generic tarball is preferred. If it is unavailable, extract it from the `mysql:8.4.6` Docker image and record the digests. |
| etcd | 3.5.17. Not ≥ 3.7: its `TryLock` cannot detect a held lock (#21548). |
| User | root runner, dropping to `RUN_USER=ubuntu` |

Known pitfalls from earlier runs:
- `sudo` drops `PATH`. Pass it with `sudo env PATH="$PATH" ...`, otherwise `chaos_run.sh` fails silently while sourcing `build.env`.
- The MySQL socket path is limited to 107 bytes. Use a short VTDATAROOT such as `/work/vd/rNN`, mapped to run IDs in `vtdata-map.txt`.
- Containers whose PID 1 reaps orphans slowly break the harness's `kill -9` and restart sequences. Run every attempt under a child subreaper (`prctl(PR_SET_CHILD_SUBREAPER)` plus a `wait4(-1)` loop), uniformly for every revision.
- Set `CHAOS_KEEP_DATA=1` to keep the runtime directories for evidence.

## Supervisor

B relies on something restarting vttablet when it exits. The harness supervises each restarted vttablet itself (`Supervisor` in `supervisor.go`; the policy comes from `CHAOS_SUPERVISOR=k8s|fast`):
- It restarts an exited vttablet with exactly the same arguments and environment. Record the command line and environment of every generation and check that they match.
- **Restore flags are off in E1, E2 and E4** (no `--restore-from-backup` or `--restore-with-clone`). With them, a restart on existing data takes the restore path, and A skips replication initialization there (as main does), which would hide the difference being measured.
- Each process generation gets its own exit handling: a fresh wait/exit channel and log file per start. Do not reuse a previous generation's channel (the channel-reuse bug recorded in earlier harness work), because B can exit several times in a row.
- Record every exit with its generation, timestamp, exit code and last error line, and classify it as **induced** (the harness restarted the process) or **startup failure** (the process exited by itself, e.g. B's `cannot initialize replication while another operation holds the shard lock`). In the harness the only induced restart is the one that puts a vttablet under supervision (generation 0, started by the cluster); every later exit is the process's own. The records are in `supervisor-<tablet>.json`, with a log per generation (`<tablet>-vttablet-stderr-gen<N>.txt`).

Run every scenario under both restart policies:
- **sup-k8s:** restart delay 10 s, doubling after each startup failure up to 5 min, reset after 10 min of uptime (Kubernetes `CrashLoopBackOff`). Induced restarts use no delay.
- **sup-fast:** restart delay 1 s, fixed (systemd `Restart=always`, `RestartSec=1`).

Keep the supervisor enabled for A as well; A is not expected to exit.

## Fixture

Profile `semisync-3vtorc` (semi-sync durability). E1, E1-vtorc, E2 and E4 use five tablets: P = zone1-100 (primary), R1 = zone2-200 and R2 = zone3-300 (restarted), R3 = zone2-400 (the reparent target), R4 = zone3-500 (an acker for R3, so that the reparent can promote R3 safely with R1 and R2 unavailable). E3 uses the usual three. Writers running throughout, with acknowledged-write accounting as in earlier reruns. VTOrc running, except where a scenario says otherwise.

### The barrier

Every counted run must show that the restarted vttablets started while the reparent held the shard lock. Restarting first and then starting the reparent does not guarantee that: startup can finish before the reparent takes the lock. So each scenario starts the reparent first, holds it in progress with a barrier, and restarts the tablets inside that window.

The barrier: on the candidates (R3 and R4 for an ERS, R3 for the PRS), a harness session holds `LOCK TABLES vt_ks.chaos_t READ` (the S12a mechanism), taken 3 s before the reparent, so their SQL threads stop applying the writers' transactions while their receivers keep the relay log growing. ERS (`--wait-replicas-timeout 120s`) and PRS (`--wait-replica-timeout 120s`) then wait for the candidates while holding the shard lock. `UNLOCK TABLES` releases it. Confirm in the vtctld log that the reparent waits at that step; if it does not, say so and describe the barrier used instead.

Required timestamps for every counted run: the reparent's shard lock acquisition (vtctld `Locking shard ks/0 for action ...`); receiver revocation on each restarted tablet (its `StopReplicationAndGetStatus` or `DemotePrimary`/`SetReplicationSource` line); each restarted vttablet's start and its initialization outcome (A: `deferring replication initialization`; B: the exit); barrier release; the reparent's unlock. A run where a restarted tablet's initialization attempt does not fall between acquisition and unlock is not counted.

## Scenarios

Tests in `ab_test.go`: `TestABE1ManualERS`, `TestABE1VTOrcERS`, `TestABE2PRS`, `TestABE3StartupRestore`, `TestABE4ControlERS`. Run each with its binaries in `bin/` and a results directory per design and policy, for example:

```
CHAOS_PROFILE=semisync-3vtorc CHAOS_SUPERVISOR=fast CHAOS_RESULTS_DIR=/home/ubuntu/chaos-results/ab-A-fast \
  go/test/endtoend/vtorc/chaos/chaos_matrix.sh TestABE1ManualERS TestABE3StartupRestore
```

Run a **pilot** first: E1 and E3 once per design under sup-fast. Check that the barrier holds the reparent, that the `OVERLAP` lines say `counted=true` with timestamps that make sense, that VTOrc detects P's block in E1-vtorc, and that the supervisor's per-generation records and logs are complete. Fix the harness before running the matrix.

### E1: replicas restart during a manual ERS against a reachable primary

1. VTOrc disabled. Set the barrier on R3 and R4. Make P's commits block on semi-sync acknowledgements by cutting every replica from P's MySQL port, keeping P's vttablet and mysqld running and reachable.
2. Run `EmergencyReparentShard` to R3. Wait for the lock acquisition and for revocation on R1 and R2.
3. Make P's MySQL port reachable from R1 and R2 again, so a receiver a restarted tablet starts could reconnect to P. Restart R1's and R2's vttablets (mysqld untouched). Keep the barrier until both have reached their initialization outcome, then release it.
4. If the ERS fails, rerun it (without the barrier) every 10 s until it succeeds, for up to 10 min from step 2.

### E1-vtorc: the same, recovered by VTOrc

As E1 with VTOrc enabled and no manual ERS: VTOrc detects P's block and runs the ERS, choosing its own candidate. R3 and R4 carry the barrier. Restart R1 and R2 after VTOrc's ERS holds the lock and has revoked their receivers. VTOrc's ERS waits for relay logs only up to its own `--wait-replicas-timeout`, so the harness releases the barrier at most 8 s after the restarts. VTOrc's later recoveries run without the barrier. If VTOrc picks R1 or R2 as its candidate, record it: on B that tablet may be down.

### E2: restarts overlapping a PRS

Set the barrier on R3 and run `PlannedReparentShard` to R3. Once PRS holds the lock and has demoted P, restart R1, then R2 (the next once the previous has reached its initialization outcome). Release the barrier after both. Record the PRS duration and the restarts caused.

### E3: startup restore whose finalization finds the shard locked

1. Back up R2. Put R1 under supervision with `--restore-from-backup` kept on every generation, drop R1's keyspace database (as S12r2 does) so it restores at startup. The harness holds the shard lock from before the restore starts until 5 s after R1 logs `Restore: starting replication at position`. Finalization starts milliseconds after the data restore, too soon to take the lock "just before" it. Record R1's finalization lock request, which must fall inside the hold.
2. Expected on A: R1 defers, stays `NOT_SERVING`, and initializes when the lock is released. On B: R1's vttablet exits; after the restart it skips the restore because mysqld has data, and initializes replication.
3. Record whether R1 ever served with its receiver stopped or without a configured source, and whether it ended replicating from the shard primary.

### E4: control

E1 without restarts or barrier, on A and B: the baseline ERS duration.

## Measurements, per run

The harness writes these into each scenario's `report.txt` (lines starting `attempt`, `OVERLAP`, `RESTARTS`, `CONNECTED TO P`, `SAFETY`, `SERVED WITHOUT REPLICATION`, `CONVERGED`/`CENSORED`, `WRITES`), with `ab-timeline.json`, `repl-samples.tsv` (every 250 ms: each tablet's source port, receiver, applier and serving state), `reparent-output.txt` and the supervisor records. Reparent attempts are paired from the `Locking`/`Unlocking shard ks/0` lines of vtctld and the VTOrcs.

- **Reparent:** each attempt's start, lock acquisition, end, outcome and error; time from the first attempt to the first success. For E1-vtorc, every recovery VTOrc started, with its outcome.
- **Restarts:** per tablet, the generations with start time, exit time, classification (induced or startup failure) and error, and the supervisor delay before each restart.
- **Convergence:** time from the first reparent attempt until every replica replicates from the new primary and serves, and until writes are acknowledged again. In the ERS scenarios the old primary is left out: its commits that were blocked on semi-sync are errant GTIDs once R3 is promoted, so it cannot rejoin on either design. The harness's final invariant check still reports it in E1, E1-vtorc and E4; that is expected on both designs and is not a difference between them. If this has not happened 10 min after the first attempt, the run is **censored**: report it as "not converged within 10 min" with its state at the limit, not as a duration.
- **Writes:** acknowledged-write loss (must be zero); the write-unavailability window.
- **Connections to P, by phase.** Classify every replica connection to P's mysqld:
  - (a) existing before that replica's revocation;
  - (b) a receiver connecting to P after its revocation while the reparent is in progress (unsafe);
  - (c) after a failed reparent, by that reparent's cleanup or by VTOrc restoring the old topology (expected);
  - (d) to P after a successful promotion of another tablet (unsafe).
  Report (b) and (d) per run, with the tablet, its generation, and what configured the connection.
- **Other safety:** any errant GTID; any replica serving while not replicating.

## Matrix

After the pilot, interleave A and B runs. E1, E1-vtorc and E3: ×3 per design per supervisor profile. E2: ×2 per design per profile. E4: ×2 per design. main: E1 ×1 under sup-k8s, for reference only.

## Deliverables

A downloadable `.tar.gz` bundle, as in earlier reruns, with an `INDEX.md`, `BUILD.md`, `environment.md`, the binaries' checksums and source SHAs, the harness fixes as patches, the run table with the required timestamps, the raw logs, the per-generation supervisor records, and a summary that states, for each scenario and profile:
- reparent success rate and time to the first success, A versus B, with the spread across runs;
- for E1-vtorc: the number of failed recoveries before the first success, the restart schedule alongside them, and whether the run converged within 10 min. More than two failed recoveries shows repetition, not an indefinite loop; call a run nonconvergent only if it was censored, and say what state it was in;
- startup failures and induced restarts per design, kept separate;
- connections to P in phases (b) and (d), on either design;
- for E3, whether B's restarted tablet came back replicating, and whether either design served without replication;
- runs excluded for missing overlap, with the reason.

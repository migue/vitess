# replicationinit

End-to-end tests of replication initialization at vttablet startup and its interaction
with reparents. VTOrc does not run unless a test starts it.

Run the package through the CI entry point, keeping the data of every run:

```
go run test.go -docker=false -keep-data -print-log reparent_replicationinit
```

Or directly, with the binaries of the tree under test first in `PATH`:

```
go test -count=1 -v ./go/test/endtoend/reparent/replicationinit/
```

The shard lock control needs the etcd version that CI installs: `TryLock` blocks instead of
failing fast on etcd 3.7 and later (#21548). A failed test keeps its `VTDATAROOT` and prints
the tails of the component logs; `test.go` removes the data of a run unless `-keep-data` is set.

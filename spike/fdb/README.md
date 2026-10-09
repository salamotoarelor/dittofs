# FoundationDB spike: can FoundationDB meet RFC 16 §4.1?

The same questions as the TiKV spike (`../tikv/README.md`), asked of
FoundationDB, with the same hot-directory benchmark. A throwaway experiment in
its own Go module, so nothing here is built or tested by the repository's
`./...`. The Go binding uses cgo against `libfdb_c`, which `./cluster.sh client`
downloads outside the repository (`$TMPDIR/dittofs-fdb-client`, or
`FDB_CLIENT_DIR`).

## Running it

```bash
./cluster.sh up              # one fdbserver process, 7.3.77, host networking (Linux, x86_64)
./cluster.sh test -count=1 -v .
./cluster.sh down

./cluster.sh up 3            # three processes, double replication, three coordinators
SPIKE_NODES=3 ./cluster.sh test -count=1 -v -run TestQ5 .
```

`./cluster.sh test` runs `go test` with the client library on the cgo paths and
`SPIKE_CLUSTER_FILE` set. `SPIKE_DURATION` and `SPIKE_MODES` are as in the TiKV
spike. The cluster's data goes to `SPIKE_DATA` (default
`~/.cache/dittofs-fdb-spike`); keep it on a real disk, not a RAM-disk `/tmp`.

Versions: FoundationDB 7.3.77, the newest release the project marks stable on
8 Oct 2026 (7.4.x and 8.0.0 are marked pre-release). The Go binding at the
`7.3.77` tag, API version 730. The release's header tarball leaves out
`fdb_c_types.h`; `cluster.sh client` fetches it from the source tree at the tag.

## Where it ran

The numbers below are from **ditto**, the same host as the TiKV spike's: 16
cores, 61 GB, Docker 29.8.2, data on RAID1 over two consumer Samsung NVMe drives
where one synced 4 KB write takes 6.2 ms. A first run on a laptop put the data
on a RAM disk by mistake; its semantic answers matched ditto's, its throughput
was 3 to 7 times ditto's and is not quoted.

## Answers

| # | Question | FoundationDB 7.3.77 | TiKV 8.5.8, for comparison |
|---|---|---|---|
| 1 | Shared `Guard` | **Yes, natively**: a read conflict key, sent with the commit, no request of its own; two guards never conflict | Partly: shared only while both are held, one request each, a primary needed |
| 2 | Commit version per key from a scan | **No**; a versionstamped value carries it, at 0–6% more write time | No; a `BatchGet` per page carries it, at ~11% more read time |
| 3 | Blind write-write conflict | **No by design**; yes with a read conflict on each written key, at no extra request | Optimistic yes; pessimistic only with a lock |
| 4 | Lost update, write skew, phantom | **All three abort one side**: strictly serializable, range reads included | Lost update yes; write skew only with locks; phantom not tested (scans untracked) |
| 5 | Commit survives losing a process | **Yes**: each of three processes killed in turn, read back after 3.0 s, 4.4 s and 6 ms | Yes: 11.1 s |
| 6 | Limits | 100 000 B per value, 10 000 B per key, ~10 MB per transaction (conflict ranges count), 5 s per transaction under commits | 8 MiB per entry, no limit per transaction |
| 7 | `Now` | Monotonic, 108–207 µs per read version, but **not time**: idle versions do not move | Monotonic, 47 µs, and it is time |
| 8 | A guard refuses a write committed after it | **No**: the guard binds only its own transaction; a writer that commits second commits. A range read conflict (`AddReadConflictRange`, or a non-snapshot range read) closes the directory-removal race | Not tested |

### 1. The guard

`AddReadConflictKey(parent)` registers the key; the commit fails with
`not_committed` if anything wrote it after the transaction's read version.
Measured (`TestQ1SharedGuard`):

- two transactions guarding one key both commit;
- a writer of a guarded key is never blocked: it commits at once, and the guard
  holder aborts at its own commit;
- the same for a key that did not exist (the create-if-absent gate);
- **a transaction that guards and writes nothing is not checked** and always
  commits. That is FoundationDB skipping conflict checks for read-only
  transactions. It changes nothing a guard protects, since such a transaction
  writes nothing, but a read-only "verify, then act outside the store" step
  (RFC 9's deleter) gets a snapshot read, not a check at commit. The same is
  true of a read-only transaction on Badger and of an optimistic one on TiKV.
  A TiKV shared lock does hold off writers for the transaction's life.

On TiKV, by contrast, a guard committed after another transaction's start fails
that transaction's later guard, so TiKV's guards are shared only while held at
the same time (`../tikv/README.md`, §1).

### 8. Commit order

Q1 measured a writer committing while a guard was open. The other order
(`TestQ8GuardOrder`): G guards `parent` and writes `child`, W writes `parent`,
both from snapshots taken before either commits, G committing first. **Both
commit.** A read conflict key checks the guarding transaction's own commit,
against writes committed before it; nothing checks a later writer against it.
On its own that is safe, since W is simply ordered after G.

It breaks RFC 7 §3.6's directory removal, which relies on the removal's write
conflicting with a create's guard. A create that commits between the removal's
snapshot and its commit is missed by an untracked (snapshot) emptiness scan,
and the removal's write of the directory does not conflict with the create's
guard: both commit, and the entry is left under a removed directory. A
tracked range read of the entries, or a snapshot scan plus
`AddReadConflictRange` over them, makes the removal fail `not_committed`, as it
should. Badger behaves the same way (`pkg/metadata/kv` on `feat/kv-layer`).

### 2. Change sequence

A range read returns key and value only, and FoundationDB keeps no per-key
commit version to ask for. `SetVersionstampedValue` writes the commit's
10-byte versionstamp into the value at commit: it matched
`GetCommittedVersion` exactly, ordered as the commits were, and 1000 writes in
ten transactions took 72.9 ms against 73.0 ms plain on one process, 136.9
against 129.5 on three. This
breaks RFC 16's "nothing stores it in a value" but not its reason: no
transaction writes a counter, the store fills the stamp. Every `Set` becomes a
versionstamp op, and a transaction cannot read back a stamped value it wrote.

### 7. `Now`

The read version is a commit counter, not a clock: on an idle cluster it did not
move across 1000 reads, and under a writer committing every millisecond it
advanced 1.1–1.2 million per second (the laptop's run saw up to 3.1 million). So FoundationDB is a backend *without* a
timestamp oracle in RFC 16's sense, and `Now` is the embedded store's rule:
the node clock clamped monotone under a ceiling in `N‖node‖clk`. The
five-second transaction limit is counted in versions too: under commits a read
6 s after the snapshot failed with `transaction_too_old`, while on an idle
cluster it succeeded.

### 5. Replication

`triple` with three processes cannot survive losing one: it places logs in three
zones, and with one gone recovery cannot recruit them, so the database stayed
unavailable until the process returned. The three-process run therefore uses
`double`: two durable copies at commit, one loss survived, which matches TiKV's
three replicas with a quorum of two. A production `triple` wants at least five
zones.

## Many creates in one directory

The workload of the TiKV spike: N workers each create an entry in one directory,
retrying the whole transaction on a conflict. Three seconds per cell.

| Mode | N=1 | N=8 | N=32 | N=128 | p99 at N=128 | retries |
|---|---|---|---|---|---|---|
| FDB ×1, guard | 142 | 1 098 | 3 590 | 10 300 | 21 ms | 0 |
| FDB ×1, guard-read | 139 | 990 | 4 100 | 10 498 | 21 ms | 0 |
| FDB ×1, parent-write | 139 | 140 | 114 | 83 | 4.9 s | 52 279 |
| FDB ×3, guard | 113 | 1 160 | 2 448 | 6 698 | 39 ms | 0 |
| FDB ×3, guard-read | 132 | 1 080 | 3 181 | 5 907 | 37 ms | 0 |
| FDB ×3, parent-write | 133 | 133 | 42 | 31 | 4.8 s | 19 738 |
| TiKV ×1, shared guard | 52 | 51 | 40 | 33 | 2.8 s | 0 |
| TiKV ×3, shared guard | 41 | 37 | 26 | 26 | 4.7 s | 0 |
| Badger, `SyncWrites`, on the NVMe | 300 | 358 | 385 | 397 | 486 ms | 0 |

- `guard` is RFC 16's create: a read conflict on the parent, the entry written
  with a read conflict on itself. `guard-read` reads the parent for real first,
  one more round trip. `parent-write` writes the parent in every create, as a
  directory mtime kept in the parent record would, which is the serialising case
  guards exist to avoid.
- **One create costs about one sync** (7 ms at N=1 against a 6.2 ms sync), where
  TiKV's costs about three.
- **Guarded creates scale with N and never retry**: 6 700–10 300 per second at
  N=128, 200 to 300 times TiKV's shared guard on the same host, at a hundredth
  of its p99. Guards cost nothing until commit, two guards never meet, and the
  commit path batches many transactions into one sync, which is what lets the
  rate grow with N while each commit still waits for the disk.
- **Faster than synced Badger** on this disk too, by 16 to 26 times at N=128:
  Badger's commits barely share a sync.
- Writing the parent serialises on both stores, as expected.

## What goes back to the RFC

1. **Change sequence (§4.1):** neither replicated candidate returns a commit
   version from a scan. On FoundationDB the store writes it into the value
   (versionstamp); on TiKV a batched point read per page returns it. §4.1 should
   allow both, or say how a move's delta is found otherwise.
2. **`Now` on a backend whose versions are not time (§4.1):** FoundationDB's
   versions advance with commits, not with the clock, so it falls under the
   "without a timestamp oracle" rule. Worth one sentence naming that case.
3. **Blind writes (§4.1):** FoundationDB detects none by design; the backend adds
   a read conflict on every written key, which costs no request.
4. **Transaction limits (RFC 6's K):** 10 MB counted with conflict ranges, so
   about 20 000–40 000 small keys; 100 kB values; 5 s of versions. Tighter than
   TiKV's, and the key budget must be derived from them, not from TiKV's.

## The trade

FoundationDB matches RFC 16's model directly (optimistic transactions, guards as
read conflicts, strict serializability) and, on a disk where a sync costs 6 ms,
is 200 to 300 times faster than TiKV on the case guards exist for. The costs:

- **cgo**: `libfdb_c` at the cluster's protocol version, on every node that runs
  the server. RFC 16 names this as the price of a strictly serializable store.
- **Tighter limits** than TiKV's, which RFC 6's key budget already bounds.
- **A versionstamp in every value** for the change sequence.

# TiKV spike: can TiKV meet RFC 16 §4.1?

A throwaway experiment, not product code. It answers whether TiKV can back the
metadata store's `KV` interface as RFC 16 §4.1 (on `docs/storage-rfcs` at
`b6c6fd26`) states it, and at what cost. Its own Go module, so nothing here is
built or tested by the repository's `./...`. The FoundationDB spike beside it
(`../fdb/README.md`) asks the same questions.

## Running it

```bash
./cluster.sh up        # PD + one TiKV node, v8.5.8, host networking (Linux)
go test -count=1 -v .  # Q1–Q4, Q6, Q7, the probes and the contention benchmark (~2 min)
./cluster.sh down

./cluster.sh up 3      # three TiKV nodes, three replicas
SPIKE_NODES=3 go test -count=1 -v -run TestQ5 .
```

`SPIKE_DURATION` (default `5s`) sets each contention cell's length;
`SPIKE_MODES` picks the modes (`shared,exclusive,optimistic,badger-mem,badger-sync`).

**Keep the data on a real disk.** The cluster's data goes to `SPIKE_DATA`
(default `~/.cache/dittofs-tikv-spike`), and the synced Badger mode's to
`SPIKE_BADGER_DIR` (default: this directory). `/tmp` is a RAM disk on many Linux
hosts, where a sync costs nothing and every commit looks fast.

Versions: TiKV and PD v8.5.8 (the newest stable release on 8 Oct 2026; v9 is
still beta). `tikv/client-go/v2` at the commit TiDB v8.5.8 pins
(`v2.0.8-0.20260803075849-c3b50791b9fb`), and its `kvproto`.

## Where it ran

The numbers below are from **ditto**: 16 cores, 61 GB, Ubuntu 26.04, Docker
29.8.2, data on RAID1 over two consumer Samsung NVMe drives
(`MZVL2512HCJQ`), where **one synced 4 KB write takes 6.2 ms** (no power-loss
protection, so a sync waits for flash). All nodes on that one host.

A first run on a laptop put the data on a RAM disk by mistake. Its semantic
answers matched ditto's except where noted; its throughput was 10 to 30 times
ditto's and is not quoted.

## Answers

| # | Question | Answer |
|---|---|---|
| 1 | Shared `Guard` | **Partly**, in pessimistic transactions only: guards held at the same time are shared, but a guard committed after another transaction's start fails that transaction's later guard (below) |
| 2 | Commit timestamp per key from `Scan` | **No** from a scan; **yes** from `Get`/`BatchGet`. One batched read per scanned page costs ~11% (1000 keys: 27 → 30 ms) |
| 3 | Blind write-write conflict | **Yes** optimistic; **no** pessimistic unless the write takes a lock: two unlocked blind writes of one key both commit, losing one |
| 4 | Lost update and write skew abort one side | Lost update **yes**. Write skew **no** under plain snapshot isolation; **yes** with reads as shared locks and writes as exclusive locks |
| 5 | A returned commit survives losing its leader | **Yes**: the region's leader killed right after the commit returned; the survivors served it 11.1 s later |
| 6 | Limits | One entry: **8 MiB** (`raft entry is too large`; 6 MiB passes). One transaction: no client-side limit (512 × 1 MiB and 10⁶ keys both committed) |
| 7 | `Now` | Monotonic, 47 µs per timestamp |

### 1. Shared guard

TiKV v8.5.8 implements shared locks (`Op_SharedLock`, `Op_SharedPessimisticLock`),
and client-go exposes them as `LockCtx.InShareMode`. Measured (`TestQ1SharedGuard`):

- two transactions guarding one key at the same time both commit;
- an exclusive lock on a guarded key is refused while the guard is held;
- an optimistic blind write of a guarded key waits for the guard's commit, then
  fails with a write conflict;
- a guard taken after a write committed past the transaction's snapshot fails
  with a write conflict, for an existing key and for an absent one (the
  create-if-absent gate).

**But a committed guard counts as a write to a later one** (`TestProbe*`): if A
guards the parent and commits after B's start, B's guard of the parent then
fails with a write conflict whose commit timestamp is A's. RFC 16's guard
"MUST NOT conflict with one that only guards it", so TiKV's shared lock meets
the rule only for guards whose lifetimes overlap, not for one that committed in
between. Whether a given guard sees it depends on timing: on the laptop all
three probes passed; on ditto two failed (one no-wait, one waiting) and a third,
no-wait, passed.

Two more constraints:

- **Pessimistic transactions only.** In an optimistic transaction client-go
  quietly takes an exclusive lock instead.
- **A shared lock cannot be the transaction's primary.** client-go refuses one
  ("pessimistic lock in share mode requires primary key to be selected") until
  an exclusive lock has chosen the primary. A create has one naturally: its new
  entry. A transaction that only guards needs an exclusive lock on a key of its
  own, which costs one more lock request.

So the backend would be: every `Update` a pessimistic transaction, a `Guard` and
every tracked `Get` a shared lock at the start timestamp, every `Set`/`Delete`
an exclusive lock first (see 3), and any write conflict retried as a whole
transaction.

### 2. Change sequence

`kvrpcpb.ScanRequest` has no `need_commit_ts` and the iterator exposes only key
and value, so no scan returns a commit timestamp. `Get` and `BatchGet` do, with
`WithReturnCommitTS()`, and the values are ordered as the commits were.

A delta (RFC 27 §2.2) can therefore read a page of keys, then one `BatchGet` with
commit timestamps for the page. That holds RFC 16's "no request per key", but not
its "`Scan` returns it in each `KeyValue`". RFC 16 should allow the extra read
per page, or the delta must be found another way.

### 3. Blind writes, pessimistic

In a pessimistic transaction, a key written without a lock is not checked
against the snapshot: two such writers of one key both committed on ditto, on
one node and on three, so one update was lost silently. (On the laptop's single
node it conflicted, so the outcome depends on the commit path and must not be
relied on.) With an exclusive lock taken first, the second writer fails with a
write conflict. **The backend's `Set` and `Delete` must lock.**

## Many creates in one directory

The case shared guards exist for (RFC 16 §4.1, RFC 7 §3.6). N workers each create
an entry in one directory: guard the parent, write the entry, commit, retry the
whole transaction on a conflict. Three seconds per cell; creates per second, and
the p99 latency of one create including its retries.

| Mode | N=1 | N=8 | N=32 | N=128 | p99 at N=128 |
|---|---|---|---|---|---|
| TiKV ×1, shared guard | 52 | 51 | 40 | 33 | 2.8 s |
| TiKV ×1, exclusive guard | 75 | 81 | 78 | 84 | 2.3 s |
| TiKV ×1, optimistic lock-only | 85 | 25 | 10 | 3 | 4.6 s |
| TiKV ×3, shared guard | 41 | 37 | 26 | 26 | 4.7 s |
| TiKV ×3, exclusive guard | 48 | 49 | 46 | 46 | 4.6 s |
| TiKV ×3, optimistic lock-only | 58 | 19 | 5 | 2 | 4.9 s |
| Badger, `SyncWrites`, on the NVMe | 300 | 358 | 385 | 397 | 486 ms |
| Badger, in memory (no disk) | 218 036 | 287 133 | 353 027 | 449 829 | 1 ms |

- **One create costs about three syncs** (19 ms at N=1 against a 6.2 ms sync):
  the entry's lock, the parent's lock, then prewrite and commit.
- **Nothing scales with N.** Shared, exclusive and optimistic guards all stay
  between 25 and 85 creates per second, with p99s of seconds. Shared guards do
  no better than exclusive ones here: a guard that waits behind another's commit
  sees it as a write (§1), so the creates queue on the parent.
- **The optimistic lock-only fallback collapses** to single digits.
- **Badger with `SyncWrites` reaches about 400 commits per second for the whole
  store**, not per directory: each commit is one sync, and concurrent commits
  barely share one. The in-memory row shows what Badger costs without the disk.

## What goes back to the RFC

1. **Shared guards (§4.1):** on TiKV a guard committed after another
   transaction's start fails that transaction's guard, so guards are shared only
   while their lifetimes overlap. For a hot directory that serialises creates on
   the parent, which is what the shared rule was written to prevent.
2. **Change sequence (§4.1):** `Scan` cannot return commit timestamps on TiKV.
   Either allow a batched point read per scanned page, or rethink how a move's
   delta is found.
3. **Shared guards need a primary (§4.1 / RFC 6 backend notes):** a lock-based
   backend's guard-only transaction pays one exclusive lock on a key of its own.
4. **Pessimistic writes must lock:** skipping the write's exclusive lock loses
   updates on TiKV. Worth stating as a MUST.
5. **One-directory create rate:** about 30–80 creates per second on this
   hardware, flat in N. FoundationDB on the same box reached 6 700–10 300
   (`../fdb/README.md`).

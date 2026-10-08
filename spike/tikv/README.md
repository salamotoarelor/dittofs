# TiKV spike: can TiKV meet RFC 16 §4.1?

A throwaway experiment, not product code. It answers whether TiKV can back the
metadata store's `KV` interface as RFC 16 §4.1 (on `docs/storage-rfcs` at
`b6c6fd26`) states it, and at what cost. Its own Go module, so nothing here is
built or tested by the repository's `./...`.

## Running it

```bash
./cluster.sh up        # PD + one TiKV node, v8.5.8, host networking (Linux)
go test -count=1 -v .  # Q1–Q4, Q6, Q7, and the contention benchmark (~2 min)
./cluster.sh down

./cluster.sh up 3      # three TiKV nodes, three replicas
SPIKE_NODES=3 go test -count=1 -v -run TestQ5 .
```

`SPIKE_DURATION` (default `5s`) sets each contention cell's length;
`SPIKE_MODES` picks the modes (`shared,exclusive,optimistic,badger-mem,badger-sync`).

Versions: TiKV and PD v8.5.8 (the newest stable release on 8 Oct 2026; v9 is
still beta). `tikv/client-go/v2` at the commit TiDB v8.5.8 pins
(`v2.0.8-0.20260803075849-c3b50791b9fb`), and its `kvproto`.

## Answers

| # | Question | Answer |
|---|---|---|
| 1 | Shared `Guard` | **Yes**, in pessimistic transactions only (below) |
| 2 | Commit timestamp per key from `Scan` | **No** from a scan; **yes** from `Get`/`BatchGet`. One batched read per scanned page costs ~13% |
| 3 | Blind write-write conflict | **Yes** optimistic; **no** pessimistic unless the write takes a lock: two unlocked blind writes both commit, losing one |
| 4 | Lost update and write skew abort one side | Lost update **yes**. Write skew **no** under plain snapshot isolation; **yes** with reads as shared locks and writes as exclusive locks |
| 5 | A returned commit survives losing its leader | **Yes**: the leader was killed right after the commit returned; the survivors served it 11.6 s later |
| 6 | Limits | One entry: **8 MiB** (`raft entry is too large`; 6 MiB passes). One transaction: no client-side limit (512 × 1 MiB and 10⁶ keys both committed) |
| 7 | `Now` | Monotonic. 79 µs per timestamp on one host |

### 1. Shared guard

TiKV v8.5.8 implements shared locks (`Op_SharedLock`, `Op_SharedPessimisticLock`),
and client-go exposes them as `LockCtx.InShareMode`. Measured (`TestQ1SharedGuard`):

- two transactions guarding one key both commit;
- an exclusive lock on a guarded key is refused while the guard is held;
- an optimistic blind write of a guarded key waits for the guard's commit, then
  fails with a write conflict;
- a guard taken after a write committed past the transaction's snapshot fails
  with a write conflict, for an existing key and for an absent one (the
  create-if-absent gate).

Two constraints come with it:

- **Pessimistic transactions only.** In an optimistic transaction client-go
  quietly takes an exclusive lock instead.
- **A shared lock cannot be the transaction's primary.** client-go refuses one
  ("pessimistic lock in share mode requires primary key to be selected") until
  an exclusive lock has chosen the primary. A create has one naturally: its new
  entry. A transaction that only guards needs an exclusive lock on a key of its
  own, which costs one more lock request.

So the backend is: every `Update` a pessimistic transaction, a `Guard` and every
tracked `Get` a shared lock at the start timestamp, every `Set`/`Delete` an
exclusive lock first (see 3), and any write conflict retried as a whole
transaction.

### 2. Change sequence

`kvrpcpb.ScanRequest` has no `need_commit_ts` and the iterator exposes only key
and value, so no scan returns a commit timestamp. `Get` and `BatchGet` do, with
`WithReturnCommitTS()`, and the values are ordered as the commits were.

A delta (RFC 27 §2.2) can therefore read a page of keys, then one `BatchGet` with
commit timestamps for the page: 1000 keys took 43 ms to scan and 48 ms with the
batch. That holds RFC 16's "no request per key", but not its "`Scan` returns it
in each `KeyValue`". RFC 16 should allow the extra read per page, or the
delta must be found another way.

### 3. Blind writes, pessimistic

In a pessimistic transaction, a key written without a lock is not checked
against the snapshot. On three nodes, two such writers of one key both committed
in 10 runs out of 10, so one update was lost silently. (On one node it
conflicted, so this depends on timing or commit path, and must not be relied on.)
With an exclusive lock taken first, the second writer fails with a write
conflict. **The backend's `Set` and `Delete` must lock.**

## Many creates in one directory

The case shared guards exist for (RFC 16 §4.1, RFC 7 §3.6). N workers each create
an entry in one directory: guard the parent, write the entry, commit, retry the
whole transaction on a conflict. Three seconds per cell; creates per second, and
p99 latency per create including its retries.

| Mode | N=1 | N=8 | N=32 | N=128 | p99 at N=32 |
|---|---|---|---|---|---|
| TiKV ×1, shared guard | 558 | 1250 | 1286 | 1067 | 41 ms |
| TiKV ×1, exclusive guard | 544 | 598 | 628 | 621 | 1.0 s |
| TiKV ×1, optimistic lock-only | 946 | 845 | 373 | 40 | 0.6 s |
| TiKV ×3, shared guard | 500 | 675 | 527 | 640 | 147 ms |
| TiKV ×3, exclusive guard | 496 | 434 | 445 | 462 | 1.6 s |
| TiKV ×3, optimistic lock-only | 678 | 521 | 327 | 38 | 0.9 s |
| Badger, in memory | 93 494 | 144 010 | 189 423 | 225 839 | <1 ms |
| Badger, `SyncWrites` | 64 417 | 105 054 | 138 865 | 181 552 | 1 ms |

All TiKV nodes run on one laptop under podman, so the absolute numbers say little;
the ratios are the result:

- **Shared guards are what make TiKV workable here.** They give 1.2 to 2 times
  the exclusive rate, at a tenth to a twenty-fifth of its p99, and they don't
  collapse with N.
  The optimistic lock-only fallback collapses: 40 creates/s at N=128.
- **Shared guards still retry.** Every retry is a write conflict on the parent
  with reason `PessimisticRetry`, which TiKV returns to a lock that waited on
  another transaction's commit. It is not a semantic conflict: a guard committed
  after another's snapshot does not fail it (`TestProbe*`). Retrying only the
  lock at the same timestamp made it worse (about 1000/s), so the backend retries
  the whole transaction.
- **Badger is two orders of magnitude faster** for this workload. That is the
  expected price of a replicated store, not a defect, but it means a hot
  directory on TiKV tops out near a thousand creates per second.

## What goes back to the RFC

1. **Change sequence (§4.1):** `Scan` cannot return commit timestamps on TiKV.
   Either allow a batched point read per scanned page, or rethink how a move's
   delta is found.
2. **Shared guards need a primary (§4.1 / RFC 6 backend notes):** a lock-based
   backend's guard-only transaction pays one exclusive lock on a key of its own.
   Worth one sentence, so nobody makes a guard the primary.
3. **Pessimistic writes must lock:** the backend notes say a write takes "the
   exclusive one"; the spike shows that skipping it loses updates on TiKV, not
   just weakens them. Worth stating as a MUST.
4. **One-directory create rate:** shared guards keep TiKV near its single-key
   commit rate rather than serialising, but that rate is about 10³/s, against
   10⁵/s for Badger. If a workload needs more per directory, it is a design
   question (sharding a directory's entries), not a backend setting.

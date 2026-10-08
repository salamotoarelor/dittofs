package spike

import (
	"context"
	"testing"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
	tikvkv "github.com/tikv/client-go/v2/kv"
)

// Q1: RFC 16 §4.1 Guard. Two guards of one key MUST NOT conflict; a guard
// MUST conflict with a concurrent write of the key. The guard is a shared
// pessimistic lock taken at the start timestamp.
func TestQ1SharedGuard(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()

	t.Run("two guards of one key both commit", func(t *testing.T) {
		k := keys(t)
		commitOne(t, c, k("parent"), "dir")
		a, b := begin(t, c, true), begin(t, c, true)
		if err := lock(ctx, a, true, tikvkv.LockNoWait, k("parent")); err != nil {
			t.Fatalf("A guard: %s", describe(err))
		}
		if err := lock(ctx, b, true, tikvkv.LockNoWait, k("parent")); err != nil {
			t.Fatalf("B guard while A holds one: %s", describe(err))
		}
		mustSet(t, a, k("a"), "1")
		mustSet(t, b, k("b"), "1")
		errA, errB := a.Commit(ctx), b.Commit(ctx)
		t.Logf("A: %s, B: %s", describe(errA), describe(errB))
		if errA != nil || errB != nil {
			t.Fatal("two shared guards conflicted")
		}
	})

	t.Run("a writer cannot lock a guarded key", func(t *testing.T) {
		k := keys(t)
		commitOne(t, c, k("parent"), "dir")
		a := begin(t, c, true)
		if err := lock(ctx, a, true, tikvkv.LockNoWait, k("parent")); err != nil {
			t.Fatalf("A guard: %s", describe(err))
		}
		w := begin(t, c, true)
		err := lock(ctx, w, false, tikvkv.LockNoWait, k("parent"))
		t.Logf("writer's exclusive lock while A guards: %s", describe(err))
		if err == nil {
			t.Fatal("exclusive lock granted over a shared guard")
		}
		_ = w.Rollback()
		if err := a.Commit(ctx); err != nil {
			t.Fatalf("A commit: %s", describe(err))
		}
	})

	t.Run("an optimistic blind writer waits for a held guard", func(t *testing.T) {
		k := keys(t)
		commitOne(t, c, k("parent"), "dir")
		a := begin(t, c, true)
		if err := lock(ctx, a, true, tikvkv.LockNoWait, k("parent")); err != nil {
			t.Fatalf("A guard: %s", describe(err))
		}
		w := begin(t, c, false)
		mustSet(t, w, k("parent"), "changed")
		done := make(chan error, 1)
		start := time.Now()
		go func() {
			cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			done <- w.Commit(cctx)
		}()
		time.Sleep(time.Second)
		errA := a.Commit(ctx)
		errW := <-done
		t.Logf("A (guard, committed after 1s): %s; writer: %s after %v",
			describe(errA), describe(errW), time.Since(start).Round(time.Millisecond))
		if errA != nil && errW != nil {
			t.Fatal("both sides failed")
		}
	})

	for _, missing := range []bool{false, true} {
		name := "a write committed after the snapshot fails a later guard"
		if missing {
			name = "a create committed after the snapshot fails a later guard of the absent key"
		}
		t.Run(name, func(t *testing.T) {
			k := keys(t)
			if !missing {
				commitOne(t, c, k("parent"), "dir")
			}
			a := begin(t, c, true)
			commitOne(t, c, k("parent"), "written after A's snapshot")
			err := lock(ctx, a, true, tikvkv.LockNoWait, k("parent"))
			t.Logf("A's guard after a newer commit: %s", describe(err))
			if !tikverr.IsErrWriteConflict(err) {
				t.Fatal("guard did not see the newer write")
			}
			_ = a.Rollback()
		})
	}
}

// Q2: RFC 16 §4.1 change sequence. Scan must return each key's commit
// timestamp.
func TestQ2ChangeSequence(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	k := keys(t)
	names := [][]byte{k("1"), k("2"), k("3")}
	for _, n := range names {
		commitOne(t, c, n, "v")
	}

	// unionstore.Iterator exposes Key and Value only, and kvrpcpb.ScanRequest
	// has no NeedCommitTs field, so a scan cannot return commit timestamps.
	snap := c.GetSnapshot(mustTS(t, c))
	it, err := snap.Iter(k(""), k("~"))
	if err != nil {
		t.Fatal(err)
	}
	var scanned [][]byte
	for it.Valid() {
		scanned = append(scanned, append([]byte(nil), it.Key()...))
		if err := it.Next(); err != nil {
			t.Fatal(err)
		}
	}
	it.Close()
	t.Logf("scan returned %d keys, no commit timestamps (API has none)", len(scanned))

	got, err := snap.BatchGet(ctx, scanned, tikvkv.WithReturnCommitTS())
	if err != nil {
		t.Fatal(err)
	}
	var prev uint64
	for _, n := range names {
		ts := got[string(n)].CommitTS
		t.Logf("BatchGet %s: commit ts %d", n[len(n)-1:], ts)
		if ts == 0 || ts <= prev {
			t.Fatalf("commit ts missing or out of order: %d after %d", ts, prev)
		}
		prev = ts
	}
	e, err := snap.Get(ctx, names[0], tikvkv.WithReturnCommitTS())
	if err != nil || e.CommitTS != got[string(names[0])].CommitTS {
		t.Fatalf("Get commit ts %d, err %v", e.CommitTS, err)
	}

	// The cost of the workaround: a key-only scan page, then one BatchGet for
	// the page.
	const n = 1000
	txn := begin(t, c, false)
	for i := 0; i < n; i++ {
		mustSet(t, txn, k("page/"+pad(i)), "value-of-some-length-0123456789")
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	snap = c.GetSnapshot(mustTS(t, c))
	start := time.Now()
	ks := scanKeys(t, snap, k("page/"), k("page/~"))
	scanOnly := time.Since(start)
	if _, err := snap.BatchGet(ctx, ks, tikvkv.WithReturnCommitTS()); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d keys: scan %v, scan + BatchGet with commit ts %v",
		len(ks), scanOnly.Round(time.Microsecond), time.Since(start).Round(time.Microsecond))
}

// Q3: a blind write-write conflict MUST abort one side.
func TestQ3BlindWrites(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	for _, mode := range []string{"optimistic", "pessimistic, no lock on the written key", "pessimistic, exclusive lock on the written key"} {
		t.Run(mode, func(t *testing.T) {
			k := keys(t)
			pess := mode != "optimistic"
			a, b := begin(t, c, pess), begin(t, c, pess)
			var errA, errB error
			if mode == "pessimistic, exclusive lock on the written key" {
				errA = lock(ctx, a, false, tikvkv.LockNoWait, k("x"))
				mustSet(t, a, k("x"), "a")
				if errA == nil {
					errA = a.Commit(ctx)
				}
				errB = lock(ctx, b, false, tikvkv.LockNoWait, k("x"))
				if errB == nil {
					mustSet(t, b, k("x"), "b")
					errB = b.Commit(ctx)
				} else {
					_ = b.Rollback()
				}
			} else {
				mustSet(t, a, k("x"), "a")
				mustSet(t, b, k("x"), "b")
				errA, errB = a.Commit(ctx), b.Commit(ctx)
			}
			t.Logf("A: %s, B: %s", describe(errA), describe(errB))
			if errA == nil && errB == nil {
				if mode == "pessimistic, no lock on the written key" {
					// Recorded, not failed: this is the hazard the notes name.
					t.Log("both blind writers committed: an update was lost")
					return
				}
				t.Error("both blind writers committed: an update was lost")
			}
		})
	}
}

// Q4: tracked point reads. A lost update and write skew over point keys MUST
// abort one side.
func TestQ4TrackedReads(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()

	t.Run("lost update, optimistic", func(t *testing.T) {
		k := keys(t)
		commitOne(t, c, k("x"), "0")
		a, b := begin(t, c, false), begin(t, c, false)
		for _, txn := range []interface {
			Get(context.Context, []byte, ...tikvkv.GetOption) (tikvkv.ValueEntry, error)
		}{a, b} {
			if _, err := txn.Get(ctx, k("x")); err != nil {
				t.Fatal(err)
			}
		}
		mustSet(t, a, k("x"), "a")
		mustSet(t, b, k("x"), "b")
		errA, errB := a.Commit(ctx), b.Commit(ctx)
		t.Logf("A: %s, B: %s", describe(errA), describe(errB))
		if errA == nil && errB == nil {
			t.Error("lost update: both committed")
		}
	})

	t.Run("write skew, optimistic (snapshot isolation allows it)", func(t *testing.T) {
		k := keys(t)
		commitOne(t, c, k("x"), "0")
		commitOne(t, c, k("y"), "0")
		a, b := begin(t, c, false), begin(t, c, false)
		_, _ = a.Get(ctx, k("x"))
		_, _ = b.Get(ctx, k("y"))
		mustSet(t, a, k("y"), "a")
		mustSet(t, b, k("x"), "b")
		errA, errB := a.Commit(ctx), b.Commit(ctx)
		t.Logf("A: %s, B: %s (both committing is the anomaly §4.1 forbids)", describe(errA), describe(errB))
	})

	t.Run("write skew, read = shared lock, write = exclusive lock", func(t *testing.T) {
		k := keys(t)
		commitOne(t, c, k("x"), "0")
		commitOne(t, c, k("y"), "0")
		a, b := begin(t, c, true), begin(t, c, true)
		// A reads x, B reads y, each under a shared lock.
		if err := lock(ctx, a, true, tikvkv.LockNoWait, k("x")); err != nil {
			t.Fatal(describe(err))
		}
		if err := lock(ctx, b, true, tikvkv.LockNoWait, k("y")); err != nil {
			t.Fatal(describe(err))
		}
		// Each then writes the key the other read.
		errA := lock(ctx, a, false, tikvkv.LockNoWait, k("y"))
		if errA == nil {
			mustSet(t, a, k("y"), "a")
			errA = a.Commit(ctx)
		} else {
			_ = a.Rollback()
		}
		errB := lock(ctx, b, false, tikvkv.LockNoWait, k("x"))
		if errB == nil {
			mustSet(t, b, k("x"), "b")
			errB = b.Commit(ctx)
		} else {
			_ = b.Rollback()
		}
		t.Logf("A: %s, B: %s", describe(errA), describe(errB))
		if errA == nil && errB == nil {
			t.Error("write skew: both committed")
		}
		if errA != nil && errB != nil {
			t.Log("both aborted: correct, but a retry is needed for progress")
		}
	})
}

// Q7: RFC 16 §4.1 Now. Monotonic across transactions, and its cost.
func TestQ7Now(t *testing.T) {
	c := newClient(t)
	const n = 1000
	var prev uint64
	start := time.Now()
	for i := 0; i < n; i++ {
		ts := mustTS(t, c)
		if ts <= prev {
			t.Fatalf("timestamp %d not above %d", ts, prev)
		}
		prev = ts
	}
	t.Logf("%d sequential timestamps, %v each", n, (time.Since(start) / n).Round(time.Microsecond))

	prev = 0
	start = time.Now()
	for i := 0; i < n; i++ {
		txn := begin(t, c, false)
		if txn.StartTS() <= prev {
			t.Fatalf("start ts %d not above %d", txn.StartTS(), prev)
		}
		prev = txn.StartTS()
		_ = txn.Rollback()
	}
	t.Logf("%d sequential Begin, %v each", n, (time.Since(start) / n).Round(time.Microsecond))
}

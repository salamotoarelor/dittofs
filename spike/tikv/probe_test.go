package spike

import (
	"context"
	"errors"
	"testing"

	tikverr "github.com/tikv/client-go/v2/error"
	tikvkv "github.com/tikv/client-go/v2/kv"
)

// Follow-ups to Q1, from chasing the retries TestCreatesInOneDirectory shows
// for shared guards. A guard committed after B's snapshot: does it fail B's
// later guard, or B's commit?
func TestProbeCommittedGuardFailsLaterGuard(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	k := keys(t)
	commitOne(t, c, k("parent"), "dir")
	b := begin(t, c, true)
	a := begin(t, c, true)
	if err := lock(ctx, a, true, tikvkv.LockNoWait, k("parent")); err != nil {
		t.Fatal(describe(err))
	}
	if err := a.Commit(ctx); err != nil {
		t.Fatal(describe(err))
	}
	err := lock(ctx, b, true, tikvkv.LockNoWait, k("parent"))
	var wc *tikverr.ErrWriteConflict
	if errors.As(err, &wc) {
		t.Logf("B's guard: write conflict on %q, B start %d, conflict start %d commit %d",
			wc.Key, wc.StartTs, wc.ConflictTs, wc.ConflictCommitTs)
	} else {
		t.Logf("B's guard: %s", describe(err))
	}
	if err != nil {
		_ = b.Rollback()
		return
	}
	mustSet(t, b, k("b-entry"), "1")
	err = b.Commit(ctx)
	if errors.As(err, &wc) {
		t.Logf("B's commit: write conflict on %q, B start %d, conflict start %d commit %d",
			wc.Key, wc.StartTs, wc.ConflictTs, wc.ConflictCommitTs)
	} else {
		t.Logf("B's commit: %s", describe(err))
	}
}

// Two guards held at once, committed one after the other.
func TestProbeOverlappingGuardsCommit(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	k := keys(t)
	commitOne(t, c, k("parent"), "dir")
	a, b := begin(t, c, true), begin(t, c, true)
	if err := lock(ctx, a, true, tikvkv.LockNoWait, k("parent")); err != nil {
		t.Fatal(describe(err))
	}
	if err := lock(ctx, b, true, tikvkv.LockNoWait, k("parent")); err != nil {
		t.Fatal(describe(err))
	}
	mustSet(t, a, k("a"), "1")
	mustSet(t, b, k("b"), "1")
	errB := b.Commit(ctx)
	errA := a.Commit(ctx)
	t.Logf("B (younger) first: %s; then A: %s", describe(errB), describe(errA))
}

// As TestProbeCommittedGuardFailsLaterGuard, for each wait mode and for a
// guard committed by a transaction whose primary is its own entry.
func TestProbeWaitModes(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	for _, wait := range []int64{tikvkv.LockNoWait, tikvkv.LockAlwaysWait} {
		k := keys(t)
		commitOne(t, c, k("parent"), "dir")
		b := begin(t, c, true)
		a := begin(t, c, true)
		if err := lock(ctx, a, true, tikvkv.LockNoWait, k("parent")); err != nil {
			t.Fatal(describe(err))
		}
		mustSet(t, a, k("a-entry"), "1")
		if err := a.Commit(ctx); err != nil {
			t.Fatal(describe(err))
		}
		err := lock(ctx, b, true, wait, k("parent"))
		t.Logf("wait=%d: B's guard after A's guard committed: %s", wait, describe(err))
		_ = b.Rollback()
	}
}

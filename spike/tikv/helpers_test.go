package spike

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
	tikvkv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

var pdAddr = envOr("SPIKE_PD", "127.0.0.1:2379")

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func newClient(t testing.TB) *txnkv.Client {
	t.Helper()
	c, err := txnkv.NewClient([]string{pdAddr})
	if err != nil {
		t.Skipf("no TiKV cluster at %s (run ./cluster.sh up): %v", pdAddr, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

var run atomic.Int64

// keys returns a key builder unique to this test run, so tests never see each
// other's keys and a rerun never sees an earlier run's.
func keys(t testing.TB) func(string) []byte {
	p := fmt.Sprintf("spike/%s/%d-%d/", t.Name(), time.Now().UnixNano(), run.Add(1))
	return func(s string) []byte { return []byte(p + s) }
}

func begin(t testing.TB, c *txnkv.Client, pessimistic bool) *transaction.KVTxn {
	t.Helper()
	txn, err := c.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	txn.SetPessimistic(pessimistic)
	if pessimistic {
		// A shared lock cannot be a transaction's primary: client-go refuses
		// one until an exclusive lock has chosen the primary. A transaction
		// with no exclusive lock of its own takes one on a key nobody else
		// uses, at the cost of one more lock request.
		pk := []byte(fmt.Sprintf("spike/txn/%d", txn.StartTS()))
		if err := lock(context.Background(), txn, false, tikvkv.LockNoWait, pk); err != nil {
			t.Fatalf("primary lock: %v", err)
		}
	}
	return txn
}

// lock takes a pessimistic lock on keys at the transaction's start timestamp,
// so a write committed after the snapshot fails the lock with a write
// conflict. shared selects a shared lock; wait is a tikvkv.Lock* wait time.
func lock(ctx context.Context, txn *transaction.KVTxn, shared bool, wait int64, ks ...[]byte) error {
	lc := tikvkv.NewLockCtx(txn.StartTS(), wait, time.Now())
	lc.InShareMode = shared
	return txn.LockKeys(ctx, lc, ks...)
}

func mustSet(t testing.TB, txn *transaction.KVTxn, k []byte, v string) {
	t.Helper()
	if err := txn.Set(k, []byte(v)); err != nil {
		t.Fatalf("set %s: %v", k, err)
	}
}

func commitOne(t testing.TB, c *txnkv.Client, k []byte, v string) {
	t.Helper()
	txn := begin(t, c, false)
	mustSet(t, txn, k, v)
	if err := txn.Commit(context.Background()); err != nil {
		t.Fatalf("commit %s: %v", k, err)
	}
}

// retryable reports whether err is a conflict an Update loop would retry.
func retryable(err error) bool {
	var dl *tikverr.ErrDeadlock
	return tikverr.IsErrWriteConflict(err) ||
		errors.As(err, &dl) ||
		errors.Is(err, tikverr.ErrLockAcquireFailAndNoWaitSet) ||
		errors.Is(err, tikverr.ErrLockWaitTimeout)
}

func describe(err error) string {
	switch {
	case err == nil:
		return "committed"
	case tikverr.IsErrWriteConflict(err):
		return "write conflict"
	case errors.Is(err, tikverr.ErrLockAcquireFailAndNoWaitSet):
		return "lock held (no wait)"
	case errors.Is(err, tikverr.ErrLockWaitTimeout):
		return "lock wait timeout"
	}
	var dl *tikverr.ErrDeadlock
	if errors.As(err, &dl) {
		return "deadlock"
	}
	return "error: " + err.Error()
}

func mustTS(t testing.TB, c *txnkv.Client) uint64 {
	t.Helper()
	ts, err := c.CurrentTimestamp("global")
	if err != nil {
		t.Fatalf("timestamp: %v", err)
	}
	return ts
}

func pad(i int) string { return fmt.Sprintf("%08d", i) }

func scanKeys(t testing.TB, snap *txnsnapshot.KVSnapshot, from, to []byte) [][]byte {
	t.Helper()
	it, err := snap.Iter(from, to)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	var out [][]byte
	for it.Valid() {
		out = append(out, append([]byte(nil), it.Key()...))
		if err := it.Next(); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

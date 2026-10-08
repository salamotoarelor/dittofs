package spike

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	tikverr "github.com/tikv/client-go/v2/error"
	tikvkv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/txnkv"
)

// The RFC 16 §4.1 case a guard exists for: many creates in one directory, each
// guarding the parent and writing its own entry. Measured three ways on TiKV
// and once on Badger.
//
//   - shared:     pessimistic, shared lock on the parent, exclusive lock on
//     the entry (as a blind write must take one)
//   - exclusive:  pessimistic, exclusive lock on the parent and the entry
//   - optimistic: an optimistic transaction with a lock-only mutation on the
//     parent (Op_Lock at prewrite)
//   - badger-mem:  an embedded update transaction that reads the parent, in
//     memory
//   - badger-sync: the same on disk with SyncWrites, the durable setting
func TestCreatesInOneDirectory(t *testing.T) {
	c := newClient(t)
	d := 5 * time.Second
	if v := os.Getenv("SPIKE_DURATION"); v != "" {
		var err error
		if d, err = time.ParseDuration(v); err != nil {
			t.Fatal(err)
		}
	}
	mem, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer mem.Close()
	// SyncWrites, as RFC 16 §4.1 requires of the embedded store.
	synced, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithSyncWrites(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer synced.Close()

	t.Logf("%-11s %4s %10s %10s %10s  %s", "mode", "N", "creates/s", "retries", "p99", "retry reasons")
	modes := []string{"shared", "exclusive", "optimistic", "badger-mem", "badger-sync"}
	if v := os.Getenv("SPIKE_MODES"); v != "" {
		modes = strings.Split(v, ",")
	}
	for _, mode := range modes {
		for _, n := range []int{1, 8, 32, 128} {
			k := keys(t)
			var create func(ctx context.Context, name []byte) error
			switch mode {
			case "badger-mem":
				create = badgerCreate(mem, k("parent"))
			case "badger-sync":
				create = badgerCreate(synced, k("parent"))
			default:
				commitOne(t, c, k("parent"), "dir")
				create = tikvCreate(c, mode, k("parent"))
			}
			ops, retries, p99, reasons := hammer(n, d, func(ctx context.Context, i int) error {
				return create(ctx, k(fmt.Sprintf("entry/%d", i)))
			})
			t.Logf("%-11s %4d %10.0f %10d %10v  %v", mode, n, float64(ops)/d.Seconds(), retries, p99.Round(time.Millisecond), reasons)
		}
	}
}

func tikvCreate(c *txnkv.Client, mode string, parent []byte) func(context.Context, []byte) error {
	return func(ctx context.Context, name []byte) error {
		txn, err := c.Begin()
		if err != nil {
			return err
		}
		switch mode {
		case "shared", "exclusive":
			txn.SetPessimistic(true)
			// The entry's exclusive lock first: it is the primary a shared
			// lock needs.
			err = lock(ctx, txn, false, tikvkv.LockAlwaysWait, name)
			if err == nil {
				err = lock(ctx, txn, mode == "shared", tikvkv.LockAlwaysWait, parent)
			}
		case "optimistic":
			err = txn.LockKeys(ctx, tikvkv.NewLockCtx(txn.StartTS(), tikvkv.LockNoWait, time.Now()), parent)
		}
		if err == nil {
			err = txn.Set(name, []byte("file"))
		}
		if err != nil {
			_ = txn.Rollback()
			return err
		}
		return txn.Commit(ctx)
	}
}

func badgerCreate(db *badger.DB, parent []byte) func(context.Context, []byte) error {
	return func(_ context.Context, name []byte) error {
		err := db.Update(func(txn *badger.Txn) error {
			if _, err := txn.Get(parent); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			return txn.Set(name, []byte("file"))
		})
		if errors.Is(err, badger.ErrConflict) {
			return errRetry
		}
		return err
	}
}

var errRetry = errors.New("retry")

// hammer runs op from n workers for d, retrying each call on a conflict, and
// returns the calls that succeeded, the retries, and the p99 latency of a call
// including its retries.
func hammer(n int, d time.Duration, op func(ctx context.Context, i int) error) (int64, int64, time.Duration, map[string]int) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	var ops, retries, seq atomic.Int64
	reasons := map[string]int{}
	var mu sync.Mutex
	var lat []time.Duration
	var wg sync.WaitGroup
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				i := int(seq.Add(1))
				start := time.Now()
				for ctx.Err() == nil {
					err := op(ctx, i)
					if err == nil {
						ops.Add(1)
						mu.Lock()
						lat = append(lat, time.Since(start))
						mu.Unlock()
						break
					}
					if !retryable(err) && !errors.Is(err, errRetry) {
						if ctx.Err() == nil {
							panic(err)
						}
						break
					}
					retries.Add(1)
					mu.Lock()
					reasons[reason(err)]++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return ops.Load(), retries.Load(), p99(lat), reasons
}

func p99(l []time.Duration) time.Duration {
	if len(l) == 0 {
		return 0
	}
	slices.Sort(l)
	return l[len(l)*99/100]
}

// reason names a retry's cause, and for a write conflict the key's last path
// element, so a conflict on the parent shows apart from one on the entry.
func reason(err error) string {
	var wc *tikverr.ErrWriteConflict
	if errors.As(err, &wc) {
		k := string(wc.Key)
		if i := strings.LastIndex(k, "/"); i >= 0 {
			k = k[i+1:]
		}
		if _, e := strconv.Atoi(k); e == nil {
			k = "entry"
		}
		rel := "conflict commit above our start"
		switch {
		case wc.ConflictCommitTs == 0:
			rel = "no conflict commit ts"
		case wc.ConflictCommitTs <= wc.StartTs:
			rel = "conflict commit at or below our start"
		}
		return fmt.Sprintf("write conflict on %s (%s, %s)", k, wc.Reason, rel)
	}
	return describe(err)
}

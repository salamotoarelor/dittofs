package spike

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// The RFC 16 §4.1 case a guard exists for: many creates in one directory, each
// guarding the parent and writing its own entry. The same workload as the TiKV
// spike's TestCreatesInOneDirectory.
//
//   - guard:        a read conflict on the parent, the entry written with a
//     read conflict on itself (the blind-write rule)
//   - guard-read:   the parent read for real (one more round trip), as a
//     backend that reads before it guards would
//   - parent-write: the parent written by every create, as a directory mtime
//     kept in the parent's record would be; this is the serialising case
//     guards avoid
func TestCreatesInOneDirectory(t *testing.T) {
	db := open(t)
	d := 5 * time.Second
	if v := os.Getenv("SPIKE_DURATION"); v != "" {
		var err error
		if d, err = time.ParseDuration(v); err != nil {
			t.Fatal(err)
		}
	}
	modes := []string{"guard", "guard-read", "parent-write"}
	if v := os.Getenv("SPIKE_MODES"); v != "" {
		modes = strings.Split(v, ",")
	}
	t.Logf("%-12s %4s %10s %10s %10s", "mode", "N", "creates/s", "retries", "p99")
	for _, mode := range modes {
		for _, n := range []int{1, 8, 32, 128} {
			k := keys(t)
			commitOne(t, db, k("parent"), "dir")
			ops, retries, p := hammer(n, d, func(i int) error {
				return create(db, mode, k("parent"), k(fmt.Sprintf("entry/%d", i)))
			})
			t.Logf("%-12s %4d %10.0f %10d %10v", mode, n, float64(ops)/d.Seconds(), retries, p.Round(time.Millisecond))
		}
	}
}

func create(db fdb.Database, mode string, parent, name fdb.Key) error {
	tr, err := db.CreateTransaction()
	if err != nil {
		return err
	}
	switch mode {
	case "guard":
		err = tr.AddReadConflictKey(parent)
	case "guard-read":
		_, err = tr.Get(parent).Get()
	case "parent-write":
		_, err = tr.Get(parent).Get()
		tr.Set(parent, []byte(name))
	}
	if err != nil {
		tr.Cancel()
		return err
	}
	if err := tr.AddReadConflictKey(name); err != nil {
		return err
	}
	tr.Set(name, []byte("file"))
	return tr.Commit().Get()
}

// hammer runs op from n workers for d, retrying each call on a conflict, and
// returns the calls that succeeded, the retries, and the p99 latency of a call
// including its retries.
func hammer(n int, d time.Duration, op func(i int) error) (int64, int64, time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	var ops, retries, seq atomic.Int64
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
					err := op(i)
					if err == nil {
						ops.Add(1)
						mu.Lock()
						lat = append(lat, time.Since(start))
						mu.Unlock()
						break
					}
					if !retryable(err) {
						panic(errors.Join(errors.New(describe(err)), err))
					}
					retries.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if len(lat) == 0 {
		return 0, retries.Load(), 0
	}
	slices.Sort(lat)
	return ops.Load(), retries.Load(), lat[len(lat)*99/100]
}

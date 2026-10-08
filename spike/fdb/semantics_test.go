package spike

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Q1: RFC 16 §4.1 Guard. Two guards of one key MUST NOT conflict; a guard
// MUST conflict with a concurrent write of the key. The guard is a read
// conflict key: no request until commit.
func TestQ1SharedGuard(t *testing.T) {
	db := open(t)

	t.Run("two guards of one key both commit", func(t *testing.T) {
		k := keys(t)
		commitOne(t, db, k("parent"), "dir")
		a, b := begin(t, db), begin(t, db)
		readVersion(t, a)
		readVersion(t, b)
		for _, tr := range []fdb.Transaction{a, b} {
			if err := tr.AddReadConflictKey(k("parent")); err != nil {
				t.Fatal(err)
			}
		}
		a.Set(k("a"), []byte("1"))
		b.Set(k("b"), []byte("1"))
		errA, errB := a.Commit().Get(), b.Commit().Get()
		t.Logf("A: %s, B: %s", describe(errA), describe(errB))
		if errA != nil || errB != nil {
			t.Fatal("two guards conflicted")
		}
	})

	t.Run("a write committed while a guard is open aborts the guard", func(t *testing.T) {
		k := keys(t)
		commitOne(t, db, k("parent"), "dir")
		a := begin(t, db)
		readVersion(t, a)
		_ = a.AddReadConflictKey(k("parent"))
		a.Set(k("a"), []byte("1"))
		commitOne(t, db, k("parent"), "changed") // never blocked
		err := a.Commit().Get()
		t.Logf("writer committed at once; A: %s", describe(err))
		if code(err) != codeNotCommitted {
			t.Fatal("guard did not see the write")
		}
	})

	t.Run("a create committed while a guard of the absent key is open aborts it", func(t *testing.T) {
		k := keys(t)
		a := begin(t, db)
		readVersion(t, a)
		_ = a.AddReadConflictKey(k("parent"))
		a.Set(k("a"), []byte("1"))
		commitOne(t, db, k("parent"), "created")
		err := a.Commit().Get()
		t.Logf("A: %s", describe(err))
		if code(err) != codeNotCommitted {
			t.Fatal("guard of an absent key did not see the create")
		}
	})

	t.Run("a guard-only transaction commits and still conflicts", func(t *testing.T) {
		k := keys(t)
		commitOne(t, db, k("parent"), "dir")
		a := begin(t, db)
		readVersion(t, a)
		_ = a.AddReadConflictKey(k("parent"))
		commitOne(t, db, k("parent"), "changed")
		err := a.Commit().Get()
		t.Logf("A, guarding and writing nothing: %s", describe(err))
		if code(err) != codeNotCommitted {
			t.Log("a read-only transaction is not checked: a guard alone needs a write to be enforced")
		}
	})
}

// Q2: RFC 16 §4.1 change sequence. FoundationDB keeps no per-key commit
// version; a versionstamped value is the store's own commit version written
// into the value at commit.
func TestQ2ChangeSequence(t *testing.T) {
	db := open(t)
	k := keys(t)
	var versions []int64
	for _, n := range []string{"1", "2", "3"} {
		tr := begin(t, db)
		tr.SetVersionstampedValue(k(n), stamped([]byte("v")))
		if err := tr.Commit().Get(); err != nil {
			t.Fatal(err)
		}
		cv, _ := tr.GetCommittedVersion()
		versions = append(versions, cv)
	}

	tr := begin(t, db)
	kvs, err := tr.GetRange(fdb.KeyRange{Begin: k(""), End: k("~")}, fdb.RangeOptions{}).GetSliceWithError()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("a range read returns key and value only; the stamp is the value's first 10 bytes")
	var prev []byte
	for i, kv := range kvs {
		stamp := kv.Value[:10]
		got := int64(binary.BigEndian.Uint64(stamp[:8]))
		t.Logf("%s: stamp version %d, commit version %d, payload %q", kv.Key[len(kv.Key)-1:], got, versions[i], kv.Value[10:])
		if got != versions[i] || bytes.Compare(stamp, prev) <= 0 {
			t.Fatalf("stamp %x does not match the commit or is out of order", stamp)
		}
		prev = stamp
	}

	// The cost: 1000 plain writes against 1000 stamped ones, in ten
	// transactions each.
	for _, mode := range []string{"plain", "stamped"} {
		start := time.Now()
		for b := 0; b < 10; b++ {
			tr := begin(t, db)
			for i := 0; i < 100; i++ {
				key := k(fmt.Sprintf("%s/%d/%d", mode, b, i))
				if mode == "plain" {
					tr.Set(key, []byte("value-of-some-length-0123456789"))
				} else {
					tr.SetVersionstampedValue(key, stamped([]byte("value-of-some-length-0123456789")))
				}
			}
			if err := tr.Commit().Get(); err != nil {
				t.Fatal(err)
			}
		}
		t.Logf("1000 %s writes in 10 transactions: %v", mode, time.Since(start).Round(time.Microsecond))
	}
}

// stamped builds a SetVersionstampedValue parameter: ten placeholder bytes the
// commit fills with its versionstamp, the payload, and the stamp's offset.
func stamped(payload []byte) []byte {
	p := make([]byte, 10, 10+len(payload)+4)
	p = append(p, payload...)
	return binary.LittleEndian.AppendUint32(p, 0)
}

// Q3: a blind write-write conflict MUST abort one side. FoundationDB checks
// reads against writes only, so a blind write needs a read conflict on its key.
func TestQ3BlindWrites(t *testing.T) {
	db := open(t)
	for _, guarded := range []bool{false, true} {
		name := map[bool]string{false: "plain blind writes", true: "each write adds a read conflict on its key"}[guarded]
		t.Run(name, func(t *testing.T) {
			k := keys(t)
			a, b := begin(t, db), begin(t, db)
			readVersion(t, a)
			readVersion(t, b)
			for _, tr := range []fdb.Transaction{a, b} {
				if guarded {
					_ = tr.AddReadConflictKey(k("x"))
				}
				tr.Set(k("x"), []byte("w"))
			}
			errA, errB := a.Commit().Get(), b.Commit().Get()
			t.Logf("A: %s, B: %s", describe(errA), describe(errB))
			if errA == nil && errB == nil {
				if !guarded {
					t.Log("both committed: expected, the last write wins and no read was lost")
					return
				}
				t.Error("both committed with read conflicts on the written key")
			}
		})
	}
}

// Q4: tracked reads. Lost update, write skew, and a phantom over a range.
func TestQ4TrackedReads(t *testing.T) {
	db := open(t)

	t.Run("lost update", func(t *testing.T) {
		k := keys(t)
		commitOne(t, db, k("x"), "0")
		a, b := begin(t, db), begin(t, db)
		_, _ = a.Get(k("x")).Get()
		_, _ = b.Get(k("x")).Get()
		a.Set(k("x"), []byte("a"))
		b.Set(k("x"), []byte("b"))
		errA, errB := a.Commit().Get(), b.Commit().Get()
		t.Logf("A: %s, B: %s", describe(errA), describe(errB))
		if errA == nil && errB == nil {
			t.Error("lost update: both committed")
		}
	})

	t.Run("write skew", func(t *testing.T) {
		k := keys(t)
		commitOne(t, db, k("x"), "0")
		commitOne(t, db, k("y"), "0")
		a, b := begin(t, db), begin(t, db)
		_, _ = a.Get(k("x")).Get()
		_, _ = b.Get(k("y")).Get()
		a.Set(k("y"), []byte("a"))
		b.Set(k("x"), []byte("b"))
		errA, errB := a.Commit().Get(), b.Commit().Get()
		t.Logf("A: %s, B: %s", describe(errA), describe(errB))
		if errA == nil && errB == nil {
			t.Error("write skew: both committed")
		}
	})

	t.Run("phantom: the directory is empty, so remove it", func(t *testing.T) {
		k := keys(t)
		a, b := begin(t, db), begin(t, db)
		kvs, err := a.GetRange(fdb.KeyRange{Begin: k("dir/"), End: k("dir/~")}, fdb.RangeOptions{}).GetSliceWithError()
		if err != nil || len(kvs) != 0 {
			t.Fatalf("expected an empty directory: %d, %v", len(kvs), err)
		}
		a.Set(k("dir-removed"), []byte("1"))
		_ = readVersion(t, b)
		b.Set(k("dir/new-entry"), []byte("1"))
		errB, errA := b.Commit().Get(), a.Commit().Get()
		t.Logf("create in the directory: %s; remove after an empty scan: %s", describe(errB), describe(errA))
		if errA == nil && errB == nil {
			t.Error("phantom: the remove committed over a create its scan missed")
		}
	})
}

// Q7: RFC 16 §4.1 Now. Monotonic across transactions, its cost, and how fast
// versions advance against the wall clock.
func TestQ7Now(t *testing.T) {
	db := open(t)
	const n = 1000
	var prev, first int64
	start := time.Now()
	for i := 0; i < n; i++ {
		tr := begin(t, db)
		v := readVersion(t, tr)
		if v < prev {
			t.Fatalf("read version %d below %d", v, prev)
		}
		if i == 0 {
			first = v
		}
		prev = v
		tr.Cancel()
	}
	el := time.Since(start)
	t.Logf("%d sequential read versions, %v each; monotonic (equal values allowed: batched)", n, (el / n).Round(time.Microsecond))
	t.Logf("idle cluster: versions advanced %.0f per second of wall clock", float64(prev-first)/el.Seconds())

	// The same with a writer committing every millisecond.
	k := keys(t)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
				commitOne(t, db, k(fmt.Sprintf("tick/%d", i)), "t")
			}
		}
	}()
	tr := begin(t, db)
	first = readVersion(t, tr)
	start = time.Now()
	time.Sleep(time.Second)
	tr = begin(t, db)
	last := readVersion(t, tr)
	el = time.Since(start)
	close(stop)
	<-done
	t.Logf("cluster with commits: versions advanced %.0f per second of wall clock", float64(last-first)/el.Seconds())
}

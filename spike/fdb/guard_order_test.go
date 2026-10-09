package spike

import (
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Q8: does a guard refuse a write that commits after the guarding
// transaction did? RFC 16 §4.1 asks a guard to conflict with every concurrent
// writer of its key, and RFC 7 §3.6's directory removal relies on it: a
// create guards the directory, the removal writes it after an untracked
// emptiness scan. Q1 measured only the writer committing first.
func TestQ8GuardOrder(t *testing.T) {
	db := open(t)

	t.Run("a writer that commits after the guard holder", func(t *testing.T) {
		k := keys(t)
		commitOne(t, db, k("parent"), "dir")
		g, w := begin(t, db), begin(t, db)
		readVersion(t, g)
		readVersion(t, w)
		_ = g.AddReadConflictKey(k("parent"))
		g.Set(k("child"), []byte("1"))
		// The writer, as the backend writes: a read conflict on the written
		// key, so blind writes conflict.
		_ = w.AddReadConflictKey(k("parent"))
		w.Set(k("parent"), []byte("changed"))
		errG := g.Commit().Get()
		errW := w.Commit().Get()
		t.Logf("guard holder: %s; writer, committing second: %s", describe(errG), describe(errW))
		if errG == nil && errW == nil {
			t.Log("both committed: a guard does not refuse a write that commits after it")
		}
	})

	// The directory removal of RFC 7 §3.6, with the create committing between
	// the removal's snapshot and its commit.
	rmdir := func(t *testing.T, scan string) {
		k := keys(t)
		commitOne(t, db, k("dir"), "d")
		rm, cr := begin(t, db), begin(t, db)
		readVersion(t, rm)
		readVersion(t, cr)
		r := fdb.KeyRange{Begin: k("dir/"), End: k("dir0")}
		var n int
		switch scan {
		case "snapshot":
			n = len(rm.Snapshot().GetRange(r, fdb.RangeOptions{}).GetSliceOrPanic())
		case "snapshot+range guard":
			n = len(rm.Snapshot().GetRange(r, fdb.RangeOptions{}).GetSliceOrPanic())
			_ = rm.AddReadConflictRange(r)
		case "tracked":
			n = len(rm.GetRange(r, fdb.RangeOptions{}).GetSliceOrPanic())
		}
		if n != 0 {
			t.Fatalf("directory not empty: %d", n)
		}
		_ = rm.AddReadConflictKey(k("dir"))
		rm.Clear(k("dir"))

		_ = cr.AddReadConflictKey(k("dir"))
		cr.Set(k("dir/f"), []byte("1"))
		_ = cr.AddReadConflictKey(k("dir/f"))
		errC := cr.Commit().Get()
		errR := rm.Commit().Get()
		t.Logf("scan %s: create %s; removal, committing second: %s", scan, describe(errC), describe(errR))
		if errC == nil && errR == nil {
			t.Log("both committed: an entry is left under a removed directory")
		}
	}
	for _, scan := range []string{"snapshot", "snapshot+range guard", "tracked"} {
		t.Run("removal after a create, scan "+scan, func(t *testing.T) { rmdir(t, scan) })
	}
}

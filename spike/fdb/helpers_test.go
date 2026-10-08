package spike

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func TestMain(m *testing.M) {
	fdb.MustAPIVersion(730)
	os.Exit(m.Run())
}

func open(t testing.TB) fdb.Database {
	t.Helper()
	cf := os.Getenv("SPIKE_CLUSTER_FILE")
	if cf == "" {
		t.Skip("run through ./cluster.sh test, which sets SPIKE_CLUSTER_FILE")
	}
	db, err := fdb.OpenDatabase(cf)
	if err != nil {
		t.Skipf("no FoundationDB at %s (run ./cluster.sh up): %v", cf, err)
	}
	// A dead cluster would otherwise block forever.
	_ = db.Options().SetTransactionTimeout(10_000)
	return db
}

var run atomic.Int64

// keys returns a key builder unique to this test run.
func keys(t testing.TB) func(string) fdb.Key {
	p := fmt.Sprintf("spike/%s/%d-%d/", t.Name(), time.Now().UnixNano(), run.Add(1))
	return func(s string) fdb.Key { return fdb.Key(p + s) }
}

func begin(t testing.TB, db fdb.Database) fdb.Transaction {
	t.Helper()
	tr, err := db.CreateTransaction()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	return tr
}

// readVersion pins the transaction's snapshot now, as a TiKV Begin does.
// FoundationDB otherwise takes it lazily at the first read.
func readVersion(t testing.TB, tr fdb.Transaction) int64 {
	t.Helper()
	v, err := tr.GetReadVersion().Get()
	if err != nil {
		t.Fatalf("read version: %v", err)
	}
	return v
}

func commitOne(t testing.TB, db fdb.Database, k fdb.Key, v string) {
	t.Helper()
	tr := begin(t, db)
	tr.Set(k, []byte(v))
	if err := tr.Commit().Get(); err != nil {
		t.Fatalf("commit %s: %v", k, err)
	}
}

const (
	codeTooOld         = 1007
	codeNotCommitted   = 1020
	codeCommitUnknown  = 1021
	codeTooLarge       = 2101
	codeKeyTooLarge    = 2102
	codeValueTooLarge  = 2103
	codeTimedOut       = 1031
	codeFutureVersion  = 1009
	codeProcessBehind  = 1037
	codeDatabaseLocked = 1038
)

func code(err error) int {
	var e fdb.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return -1
}

// retryable reports whether err is one an Update loop retries.
func retryable(err error) bool {
	switch code(err) {
	case codeNotCommitted, codeTooOld, codeCommitUnknown, codeFutureVersion, codeProcessBehind:
		return true
	}
	return false
}

func describe(err error) string {
	if err == nil {
		return "committed"
	}
	switch code(err) {
	case codeNotCommitted:
		return "conflict (not_committed)"
	case codeTooOld:
		return "transaction_too_old"
	case codeTooLarge:
		return "transaction_too_large"
	case codeValueTooLarge:
		return "value_too_large"
	case codeKeyTooLarge:
		return "key_too_large"
	}
	return "error: " + err.Error()
}

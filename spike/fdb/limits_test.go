package spike

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

// Q6: what Limits() would report: one value, one key, one transaction's size,
// and one transaction's age.
func TestQ6Limits(t *testing.T) {
	db := open(t)
	k := keys(t)

	for _, n := range []int{10_000, 99_999, 100_000, 100_001} {
		tr := begin(t, db)
		tr.Set(k(fmt.Sprintf("value/%d", n)), bytes.Repeat([]byte{'v'}, n))
		t.Logf("one %7d B value: %s", n, describe(tr.Commit().Get()))
	}
	for _, n := range []int{1_000, 9_999, 10_000, 10_001} {
		tr := begin(t, db)
		key := append(k("key/"), bytes.Repeat([]byte{'k'}, n)...)[:n]
		tr.Set(key, []byte("v"))
		t.Logf("one %6d B key: %s", n, describe(tr.Commit().Get()))
	}
	for _, mb := range []int{5, 8, 9, 10, 20} {
		tr := begin(t, db)
		for i := 0; i < mb*10; i++ {
			tr.Set(k(fmt.Sprintf("total/%d/%d", mb, i)), bytes.Repeat([]byte{'v'}, 99_000))
		}
		t.Logf("one transaction of ~%2d MB in 99 kB values: %s", mb, describe(tr.Commit().Get()))
	}
	for _, n := range []int{10_000, 20_000, 40_000, 50_000} {
		tr := begin(t, db)
		for i := 0; i < n; i++ {
			tr.Set(k(fmt.Sprintf("count/%d/%08d", n, i)), []byte("v"))
		}
		start := time.Now()
		err := tr.Commit().Get()
		t.Logf("one transaction of %6d small keys (%d B each): %s in %v", n, len(k("count/00000/00000000")), describe(err), time.Since(start).Round(time.Millisecond))
	}

	// The five-second limit is counted in versions, which advance only while
	// something commits; so measure it with a writer committing every
	// millisecond.
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
	defer func() { close(stop); <-done }()
	for _, age := range []time.Duration{2 * time.Second, 4 * time.Second, 6 * time.Second} {
		tr := begin(t, db)
		readVersion(t, tr)
		time.Sleep(age)
		_, err := tr.Get(k("value/10000")).Get()
		t.Logf("a read %v after the snapshot, under commits: %s", age, describe(err))
	}
}

// Q5: a returned commit survives killing a process. Needs ./cluster.sh up 3
// (double replication, three coordinators) and SPIKE_NODES=3.
func TestQ5Durability(t *testing.T) {
	if os.Getenv("SPIKE_NODES") != "3" {
		t.Skip("needs three processes: ./cluster.sh up 3 and SPIKE_NODES=3")
	}
	db := open(t)
	k := keys(t)
	commitOne(t, db, k("durable"), "survives")

	for node := 1; node <= 3; node++ {
		if out, err := exec.Command("./cluster.sh", "kill", fmt.Sprint(node)).CombinedOutput(); err != nil {
			t.Fatalf("kill %d: %v %s", node, err, out)
		}
		start := time.Now()
		for {
			tr := begin(t, db)
			v, err := tr.Get(k("durable")).Get()
			if err == nil {
				if string(v) != "survives" {
					t.Fatalf("read %q", v)
				}
				t.Logf("process %d killed: read back after %v", node, time.Since(start).Round(time.Millisecond))
				break
			}
			if time.Since(start) > 2*time.Minute {
				t.Fatalf("process %d killed: not readable: %v", node, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
		if out, err := exec.Command("./cluster.sh", "start", fmt.Sprint(node)).CombinedOutput(); err != nil {
			t.Fatalf("start %d: %v %s", node, err, out)
		}
		time.Sleep(5 * time.Second)
	}
}

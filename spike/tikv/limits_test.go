package spike

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/tikv/client-go/v2/util/codec"
)

// Q6: what Limits() would report. One entry, then one transaction.
func TestQ6Limits(t *testing.T) {
	c := newClient(t)
	ctx := context.Background()
	k := keys(t)

	for _, mb := range []int{1, 4, 6, 8, 16, 64} {
		txn := begin(t, c, false)
		err := txn.Set(k(fmt.Sprintf("entry/%d", mb)), bytes.Repeat([]byte{'v'}, mb<<20))
		if err == nil {
			err = txn.Commit(ctx)
		} else {
			_ = txn.Rollback()
		}
		t.Logf("one %3d MiB value: %s", mb, describe(err))
	}

	for _, total := range []int{64, 128, 256, 512} {
		txn := begin(t, c, false)
		var err error
		for i := 0; i < total && err == nil; i++ {
			err = txn.Set(k(fmt.Sprintf("total/%d/%d", total, i)), bytes.Repeat([]byte{'v'}, 1<<20))
		}
		if err == nil {
			err = txn.Commit(ctx)
		} else {
			_ = txn.Rollback()
		}
		t.Logf("one transaction of %3d x 1 MiB: %s", total, describe(err))
	}

	for _, n := range []int{10_000, 100_000, 1_000_000} {
		txn := begin(t, c, false)
		var err error
		for i := 0; i < n && err == nil; i++ {
			err = txn.Set(k(fmt.Sprintf("count/%d/%s", n, pad(i))), []byte("v"))
		}
		start := time.Now()
		if err == nil {
			err = txn.Commit(ctx)
		} else {
			_ = txn.Rollback()
		}
		t.Logf("one transaction of %7d small keys: %s in %v", n, describe(err), time.Since(start).Round(time.Millisecond))
	}
}

// Q5: a returned commit survives killing the node that led its region. Needs
// ./cluster.sh up 3 and SPIKE_NODES=3.
func TestQ5Durability(t *testing.T) {
	if os.Getenv("SPIKE_NODES") != "3" {
		t.Skip("needs a three-node cluster: ./cluster.sh up 3 and SPIKE_NODES=3")
	}
	c := newClient(t)
	k := keys(t)
	key := k("durable")
	commitOne(t, c, key, "survives")

	node := leaderNode(t, key)
	if out, err := exec.Command("./cluster.sh", "kill", fmt.Sprint(node)).CombinedOutput(); err != nil {
		t.Fatalf("kill node %d: %v %s", node, err, out)
	}
	t.Logf("killed node %d, the leader of the key's region, right after the commit returned", node)
	t.Cleanup(func() { _ = exec.Command("./cluster.sh", "start", fmt.Sprint(node)).Run() })

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for {
		snap := c.GetSnapshot(mustTS(t, c))
		e, err := snap.Get(ctx, key)
		if err == nil {
			if string(e.Value) != "survives" {
				t.Fatalf("read %q", e.Value)
			}
			t.Logf("read back from the survivors after %v", time.Since(start).Round(time.Millisecond))
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("not readable after the leader's loss: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// leaderNode returns the cluster.sh node number whose store leads the region
// holding key, from PD's HTTP API. Region keys are the memcomparable encoding
// of the transaction key.
func leaderNode(t *testing.T, key []byte) int {
	t.Helper()
	var regions struct {
		Regions []struct {
			StartKey string `json:"start_key"`
			EndKey   string `json:"end_key"`
			Leader   struct {
				StoreID uint64 `json:"store_id"`
			} `json:"leader"`
		} `json:"regions"`
	}
	var stores struct {
		Stores []struct {
			Store struct {
				ID      uint64 `json:"id"`
				Address string `json:"address"`
			} `json:"store"`
		} `json:"stores"`
	}
	getJSON(t, "/pd/api/v1/regions", &regions)
	getJSON(t, "/pd/api/v1/stores", &stores)

	enc := strings.ToUpper(hex.EncodeToString(codec.EncodeBytes(nil, key)))
	var store uint64
	for _, r := range regions.Regions {
		if r.StartKey <= enc && (r.EndKey == "" || enc < r.EndKey) {
			store = r.Leader.StoreID
		}
	}
	for _, s := range stores.Stores {
		if s.Store.ID == store {
			var port int
			_, _ = fmt.Sscanf(s.Store.Address[strings.LastIndex(s.Store.Address, ":")+1:], "%d", &port)
			return port - 20159
		}
	}
	t.Fatalf("no leader found for the key's region (store %d)", store)
	return 0
}

func getJSON(t *testing.T, path string, v any) {
	t.Helper()
	resp, err := http.Get("http://" + pdAddr + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

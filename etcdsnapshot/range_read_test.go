package etcdsnapshot

import (
	"context"
	"fmt"
	"testing"

	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/mvcc"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
)

func TestRangeRead(t *testing.T) {
	etcdBackend := backend.NewDefaultBackend("/home/tjungblu/Downloads/etcd.snapshot")
	store := mvcc.NewStore(nil, etcdBackend, &lease.FakeLessor{}, mvcc.StoreConfig{CompactionBatchLimit: 0})
	fmt.Printf("restore store with rev %d\n", store.Rev())
	result, err := store.Range(context.TODO(), []byte{}, []byte{}, mvcc.RangeOptions{Limit: -1})
	if err != nil {
		fmt.Printf("got an error while requesting whole range: %v\n", err)
		t.Fatal(err)
	}

	fmt.Printf("found %d records in snapshot\n", result.Count)
}

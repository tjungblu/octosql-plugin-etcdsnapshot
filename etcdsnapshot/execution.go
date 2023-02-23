package etcdsnapshot

import (
	"fmt"
	"strings"
	"time"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/mvcc"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
)

type DatasourceExecuting struct {
	path string
	// those are the field indices we need to include in the result
	fieldIndices []int
}

func (d *DatasourceExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	etcdBackend := backend.NewDefaultBackend(d.path)
	fmt.Printf("etcd backend read from %s with size %d\n", d.path, etcdBackend.Size())
	store := mvcc.NewStore(nil, etcdBackend, &lease.FakeLessor{}, mvcc.StoreConfig{CompactionBatchLimit: 0})
	fmt.Printf("restore store with rev %d\n", store.Rev())
	result, err := store.Range(ctx.Context, []byte{}, []byte{}, mvcc.RangeOptions{Limit: -1})
	if err != nil {
		fmt.Printf("got an error while requesting whole range: %v\n", err)
		return err
	}

	fmt.Printf("found %d records in snapshot\n", result.Count)

	// TODO(thomas): do something with the json in the value "kv.Value"
	for _, kv := range result.KVs {
		skey := string(kv.Key)
		keyPart := strings.Split(skey, "/")
		// since the keypart usually starts with /, we can remove the zero length entry at 0
		if len(keyPart) > 0 && keyPart[0] == "" {
			keyPart = keyPart[1:]
		}

		var values []octosql.Value
		if len(keyPart) == 1 {
			values = []octosql.Value{
				octosql.NewString(skey),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
			}
		} else if len(keyPart) == 3 {
			values = []octosql.Value{
				octosql.NewString(skey),
				octosql.NewString(keyPart[0]),
				octosql.NewNull(),
				octosql.NewString(keyPart[1]),
				octosql.NewNull(),
				octosql.NewString(keyPart[2]),
			}
		} else if len(keyPart) == 4 {
			values = []octosql.Value{
				octosql.NewString(skey),
				octosql.NewString(keyPart[0]),
				octosql.NewNull(),
				octosql.NewString(keyPart[1]),
				octosql.NewString(keyPart[2]),
				octosql.NewString(keyPart[3]),
			}
		} else if len(keyPart) == 5 {
			values = []octosql.Value{
				octosql.NewString(skey),
				octosql.NewString(keyPart[0]),
				octosql.NewString(keyPart[1]),
				octosql.NewString(keyPart[2]),
				octosql.NewString(keyPart[3]),
				octosql.NewString(keyPart[4]),
			}
		} else {
			fmt.Printf("couldn't parse key [%s] into schema with [%d] split %v\n", skey, len(keyPart), keyPart)
		}

		// add the size in bytes for the value, for easier sizing queries
		values = append(values, octosql.NewInt(len(kv.Value)))

		// remove the fields we don't need for a given query
		var result []octosql.Value
		for _, fi := range d.fieldIndices {
			result = append(result, values[fi])
		}

		err := produce(ProduceFromExecutionContext(ctx), NewRecord(result, false, time.Time{}))
		if err != nil {
			fmt.Printf("got an error while producing record: %v\n", err)
			return err
		}
	}

	return nil
}

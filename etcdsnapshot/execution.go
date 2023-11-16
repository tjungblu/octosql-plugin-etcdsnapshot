package etcdsnapshot

import (
	"fmt"
	"os"
	"path"
	"strings"
	"time"
	"unicode/utf8"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/server/v3/lease"
	"go.etcd.io/etcd/server/v3/mvcc"
	"go.etcd.io/etcd/server/v3/mvcc/backend"
)

type DatasourceExecuting struct {
	path string

	// those are the field indices we need to include in the result
	fieldIndices []int
	schema       Schema
}

func (d *DatasourceExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {

	stat, err := os.Stat(d.path)
	if err != nil {
		fmt.Printf("got an error while accessing db: %v\n", err)
		return err
	}

	if stat.IsDir() {
		dbPath := path.Join(d.path, "member", "snap", "db")
		_, err = os.Stat(dbPath)
		if err != nil {
			if os.IsNotExist(err) {
				fmt.Printf("found a dir, but no database file in 'member/snap/db': %v\n", err)
				return fmt.Errorf("db file not found in directory structure")
			}

			fmt.Printf("stat error with 'member/snap/db': %v\n", err)
			return err
		}

		// TODO(thomas): can we create the server instead, replay WAL and create a snapshot?

		// the DB file itself is a bbolt snapshot, so we can directly read from it the same way
		return produceFromBBoltBackend(ctx, produce, dbPath, d.fieldIndices, d.schema)
	}

	return produceFromBBoltBackend(ctx, produce, d.path, d.fieldIndices, d.schema)
}

func produceFromBBoltBackend(ctx ExecutionContext, produce ProduceFn, snapshotPath string, fieldIndices []int, schema Schema) error {
	etcdBackend := backend.NewDefaultBackend(snapshotPath)
	fmt.Printf("etcd backend read from [%s] with size %d bytes, in use: %d\n", snapshotPath, etcdBackend.Size(), etcdBackend.SizeInUse())

	var err error
	switch schema {
	case SchemaMeta:
		err = produceMetaFromBackend(ctx, produce, etcdBackend, fieldIndices)
	case SchemaContent:
		err = produceContentFromMvccStore(ctx, produce, etcdBackend, fieldIndices)
	}

	return err
}

func produceMetaFromBackend(ctx ExecutionContext, produce ProduceFn, etcdBackend backend.Backend, fieldIndices []int) error {

	values := []octosql.Value{
		octosql.NewFloat(float64(etcdBackend.Size())),
		octosql.NewFloat(float64(etcdBackend.SizeInUse())),
		octosql.NewFloat(float64(etcdBackend.Size() - etcdBackend.SizeInUse())),
	}

	// remove the fields we don't need for a given query
	var result []octosql.Value
	for _, fi := range fieldIndices {
		result = append(result, values[fi])
	}

	err := produce(ProduceFromExecutionContext(ctx), NewRecord(result, false, time.Time{}))
	if err != nil {
		fmt.Printf("got an error while producing record: %v\n", err)
		return err
	}

	return nil
}

func produceContentFromMvccStore(ctx ExecutionContext, produce ProduceFn, etcdBackend backend.Backend, fieldIndices []int) error {
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
		values := mapEtcdToOctosql(kv)

		// remove the fields we don't need for a given query
		var result []octosql.Value
		for _, fi := range fieldIndices {
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

func mapEtcdToOctosql(kv mvccpb.KeyValue) []octosql.Value {
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
		fmt.Printf("couldn't parse key [%s] into schema with len=[%d] split=%v, assuming null row\n", skey, len(keyPart), keyPart)
		values = []octosql.Value{
			octosql.NewString(skey),
			octosql.NewNull(),
			octosql.NewNull(),
			octosql.NewNull(),
			octosql.NewNull(),
			octosql.NewNull(),
		}
	}

	value := ""
	if utf8.Valid(kv.Value) {
		value = string(kv.Value)
	}

	// add the value and its size in bytes for the value, for easier sizing queries
	values = append(values, octosql.NewString(value), octosql.NewInt(len(kv.Value)))
	return values
}

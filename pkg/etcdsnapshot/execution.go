package etcdsnapshot

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path"
	"strings"
	"time"
	"unicode/utf8"

	"go.etcd.io/etcd/server/v3/mvcc/buckets"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"go.etcd.io/etcd/api/v3/mvccpb"
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
	defer etcdBackend.Close()
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
	// Get basic size information
	size := etcdBackend.Size()
	sizeInUse := etcdBackend.SizeInUse()
	sizeFree := size - sizeInUse

	stats := calculateEtcdStats(etcdBackend)

	// Calculate derived metrics
	fragmentationRatio := float64(sizeFree) / float64(size)
	defaultQuota := int64(8 * 1024 * 1024 * 1024) // 8GB default
	quotaUsageRatio := float64(size) / float64(defaultQuota)
	quotaUsagePercent := quotaUsageRatio * 100
	quotaRemaining := defaultQuota - size

	values := []octosql.Value{
		// Basic storage info
		octosql.NewInt(int(size)),
		octosql.NewInt(int(sizeInUse)),
		octosql.NewInt(int(sizeFree)),

		// Defragmentation metrics
		octosql.NewFloat(fragmentationRatio),
		octosql.NewInt(int(sizeFree)),

		// Compaction metrics
		octosql.NewInt(stats.totalKeys),
		octosql.NewInt(stats.totalRevisions),
		octosql.NewInt(stats.maxRevision),
		octosql.NewInt(stats.minRevision),
		octosql.NewInt(stats.maxRevision - stats.minRevision),
		octosql.NewFloat(stats.avgRevisionsPerKey),

		// Storage quota info
		octosql.NewInt(int(defaultQuota)),
		octosql.NewFloat(quotaUsageRatio),
		octosql.NewFloat(quotaUsagePercent),
		octosql.NewInt(int(quotaRemaining)),

		// Value size statistics
		octosql.NewInt(stats.totalValueSize),
		octosql.NewInt(stats.averageValueSize),
		octosql.NewInt(stats.largestValueSize),
		octosql.NewInt(stats.smallestValueSize),

		// Key distribution
		octosql.NewInt(stats.keysWithMultipleRevisions),
		octosql.NewInt(stats.uniqueKeys),
		octosql.NewInt(stats.keysWithLeases),
		octosql.NewInt(stats.activeLeases),
		octosql.NewInt(stats.estimatedCompactionSavings),
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
	keys, vals := etcdBackend.ReadTx().UnsafeRange(buckets.Key, revToBytes(0, 0), revToBytes(math.MaxInt64, math.MaxInt64), math.MaxInt64)
	fmt.Printf("found %d records in snapshot\n", len(keys))

	kv := mvccpb.KeyValue{}
	for i := 0; i < len(keys); i++ {
		err := kv.Unmarshal(vals[i])
		if err != nil {
			fmt.Printf("got an error while unmarshaling value: %v\n", err)
			return err
		}

		values := mapEtcdToOctosql(kv)

		// remove the fields we don't need for a given query
		var result []octosql.Value
		for _, fi := range fieldIndices {
			if fi < len(values) {
				result = append(result, values[fi])
			}
		}

		err = produce(ProduceFromExecutionContext(ctx), NewRecord(result, false, time.Time{}))
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

	values = append(values, octosql.NewFloat(float64(kv.CreateRevision)))
	values = append(values, octosql.NewFloat(float64(kv.ModRevision)))
	values = append(values, octosql.NewFloat(float64(kv.Version)))
	values = append(values, octosql.NewFloat(float64(kv.Lease)))

	value := ""
	if utf8.Valid(kv.Value) {
		value = string(kv.Value)
	}

	// add the value and its size in bytes for the value, for easier sizing queries
	values = append(values, octosql.NewString(value), octosql.NewInt(len(kv.Value)))
	return values
}

func revToBytes(main, sub int64) []byte {
	bytes := make([]byte, 17)
	binary.BigEndian.PutUint64(bytes, uint64(main))
	bytes[8] = '_'
	binary.BigEndian.PutUint64(bytes[9:], uint64(sub))
	return bytes
}

func bytesToRev(bytes []byte) (main, sub int64) {
	return int64(binary.BigEndian.Uint64(bytes[0:8])), int64(binary.BigEndian.Uint64(bytes[9:]))
}

type EtcdStats struct {
	totalKeys                  int
	totalRevisions             int
	maxRevision                int
	minRevision                int
	avgRevisionsPerKey         float64
	totalValueSize             int
	averageValueSize           int
	largestValueSize           int
	smallestValueSize          int
	keysWithMultipleRevisions  int
	uniqueKeys                 int
	keysWithLeases             int
	activeLeases               int
	estimatedCompactionSavings int
}

func calculateEtcdStats(etcdBackend backend.Backend) EtcdStats {
	stats := EtcdStats{
		minRevision:       math.MaxInt32,
		smallestValueSize: math.MaxInt32,
	}

	totalValueSize := 0
	uniqueLeases := make(map[int64]bool)
	uniqueRevisions := make(map[int64]bool) // Track unique revision numbers

	// Track total value sizes per key
	keyValueSums := make(map[string]int)
	keyRevisionCounts := make(map[string]int)

	keys, vals := etcdBackend.ReadTx().UnsafeRange(buckets.Key, revToBytes(0, 0), revToBytes(math.MaxInt64, math.MaxInt64), math.MaxInt64)
	for i := 0; i < len(keys); i++ {
		kv := mvccpb.KeyValue{}
		if err := kv.Unmarshal(vals[i]); err != nil {
			continue
		}

		stats.totalKeys++

		// Track unique revisions
		uniqueRevisions[kv.ModRevision] = true

		// Track revision ranges
		if int(kv.ModRevision) > stats.maxRevision {
			stats.maxRevision = int(kv.ModRevision)
		}
		if int(kv.CreateRevision) < stats.minRevision {
			stats.minRevision = int(kv.ModRevision)
		}

		// Track value sizes
		valueSize := len(kv.Value)
		totalValueSize += valueSize
		if valueSize > stats.largestValueSize {
			stats.largestValueSize = valueSize
		}
		if valueSize < stats.smallestValueSize {
			stats.smallestValueSize = valueSize
		}

		// Track per-key sums and counts
		key := string(kv.Key)
		keyValueSums[key] += valueSize
		keyRevisionCounts[key]++

		// Track leases
		if kv.Lease != 0 {
			stats.keysWithLeases++
			uniqueLeases[kv.Lease] = true
		}
	}

	// Calculate derived stats
	stats.uniqueKeys = len(keyValueSums)
	stats.activeLeases = len(uniqueLeases)
	stats.totalValueSize = totalValueSize
	stats.totalRevisions = len(uniqueRevisions) // Now correctly counts unique revisions

	if stats.totalKeys > 0 {
		stats.avgRevisionsPerKey = float64(stats.totalRevisions) / float64(stats.uniqueKeys) // Also fix this calculation
		stats.averageValueSize = totalValueSize / stats.totalKeys
	}

	// Calculate compaction savings: sum of all values for keys with multiple revisions
	compactionSavings := 0
	for key, totalSize := range keyValueSums {
		if keyRevisionCounts[key] > 1 {
			stats.keysWithMultipleRevisions++
			compactionSavings += totalSize
		}
	}
	stats.estimatedCompactionSavings = compactionSavings

	return stats
}

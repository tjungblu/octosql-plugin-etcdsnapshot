package etcdsnapshot

import (
	"context"
	"testing"
	"time"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestMappingOfKeys(t *testing.T) {
	scenarios := map[string]struct {
		key      string
		value    string
		expected []octosql.Value
	}{
		"none": {
			key:   "nullrow",
			value: "some",
			expected: []octosql.Value{
				octosql.NewString("nullrow"),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewInt(4),
			},
		},
		"tooMany": {
			key:   "/1/2/3/4/5/6/7",
			value: "some",
			expected: []octosql.Value{
				octosql.NewString("/1/2/3/4/5/6/7"),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewInt(4),
			},
		},
		"toplevel": {
			key:   "/test",
			value: "some",
			expected: []octosql.Value{
				octosql.NewString("/test"),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewInt(4),
			},
		},
		"three-fields": {
			key:   "/1/2/3",
			value: "some",
			expected: []octosql.Value{
				octosql.NewString("/1/2/3"),
				octosql.NewString("1"),
				octosql.NewNull(),
				octosql.NewString("2"),
				octosql.NewNull(),
				octosql.NewString("3"),
				octosql.NewInt(4),
			},
		},
		"four-fields": {
			key:   "/1/2/3/4",
			value: "some-other",
			expected: []octosql.Value{
				octosql.NewString("/1/2/3/4"),
				octosql.NewString("1"),
				octosql.NewNull(),
				octosql.NewString("2"),
				octosql.NewString("3"),
				octosql.NewString("4"),
				octosql.NewInt(10),
			},
		},
		"five-fields": {
			key:   "/1/2/3/4/5",
			value: "some-other",
			expected: []octosql.Value{
				octosql.NewString("/1/2/3/4/5"),
				octosql.NewString("1"),
				octosql.NewString("2"),
				octosql.NewString("3"),
				octosql.NewString("4"),
				octosql.NewString("5"),
				octosql.NewInt(10),
			},
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			kv := mvccpb.KeyValue{Key: []byte(scenario.key), Value: []byte(scenario.value)}
			res := mapEtcdToOctosql(kv)
			require.EqualValues(t, scenario.expected, res)
		})
	}
}

func TestBasicSnapshot(t *testing.T) {
	ds := &DatasourceExecuting{
		path:         "data/basic.snapshot",
		fieldIndices: []int{0, 1, 2, 3, 4, 5, 6},
	}

	expectedRecords := []execution.Record{
		execution.NewRecord(
			[]octosql.Value{
				octosql.NewString("a"),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewInt(1),
			}, false, time.Time{}),

		execution.NewRecord(
			[]octosql.Value{
				octosql.NewString("b"),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewInt(1),
			}, false, time.Time{}),
		execution.NewRecord(
			[]octosql.Value{
				octosql.NewString("d"),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewInt(1),
			}, false, time.Time{}),
	}

	var records []execution.Record
	err := ds.Run(execution.ExecutionContext{
		Context:         context.TODO(),
		VariableContext: nil,
	},
		func(ctx execution.ProduceContext, record execution.Record) error {
			records = append(records, record)
			return nil
		},
		nil,
	)

	require.NoError(t, err)
	require.EqualValues(t, expectedRecords, records)
}

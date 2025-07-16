package etcdsnapshot

import (
	"context"
	"fmt"
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
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewString("some"),
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
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewString("some"),
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
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewString("some"),
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
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewString("some"),
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
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewString("some-other"),
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
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewString("some-other"),
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

func TestMappingOfKeysWithRevisions(t *testing.T) {
	scenarios := map[string]struct {
		key            string
		value          string
		createRevision int64
		modRevision    int64
		version        int64
		lease          int64
		expected       []octosql.Value
	}{
		"withRevisions": {
			key:            "/kubernetes.io/pods/default/test",
			value:          "podData",
			createRevision: 100,
			modRevision:    200,
			version:        1,
			lease:          5,
			expected: []octosql.Value{
				octosql.NewString("/kubernetes.io/pods/default/test"),
				octosql.NewString("kubernetes.io"),
				octosql.NewNull(),
				octosql.NewString("pods"),
				octosql.NewString("default"),
				octosql.NewString("test"),
				octosql.NewFloat(100),
				octosql.NewFloat(200),
				octosql.NewFloat(1),
				octosql.NewFloat(5),
				octosql.NewString("podData"),
				octosql.NewInt(7),
			},
		},
		"zeroRevisions": {
			key:            "/test",
			value:          "data",
			createRevision: 0,
			modRevision:    0,
			version:        0,
			lease:          0,
			expected: []octosql.Value{
				octosql.NewString("/test"),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewNull(),
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewFloat(0),
				octosql.NewString("data"),
				octosql.NewInt(4),
			},
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			kv := mvccpb.KeyValue{
				Key:            []byte(scenario.key),
				Value:          []byte(scenario.value),
				CreateRevision: scenario.createRevision,
				ModRevision:    scenario.modRevision,
				Version:        scenario.version,
				Lease:          scenario.lease,
			}
			res := mapEtcdToOctosql(kv)
			require.EqualValues(t, scenario.expected, res)
		})
	}
}

func TestMappingOfKeysWithInvalidUTF8(t *testing.T) {
	invalidUTF8 := []byte{0xFF, 0xFE, 0xFD}
	kv := mvccpb.KeyValue{
		Key:   []byte("/test"),
		Value: invalidUTF8,
	}

	res := mapEtcdToOctosql(kv)

	// Should have empty string for invalid UTF-8
	require.Equal(t, octosql.NewString(""), res[10])
	require.Equal(t, octosql.NewInt(3), res[11]) // Length should still be correct
}

func TestMappingOfKeysWithEmptyValue(t *testing.T) {
	kv := mvccpb.KeyValue{
		Key:   []byte("/test"),
		Value: []byte{},
	}

	res := mapEtcdToOctosql(kv)

	require.Equal(t, octosql.NewString(""), res[10])
	require.Equal(t, octosql.NewInt(0), res[11])
}

func TestRevToBytesAndBack(t *testing.T) {
	testCases := []struct {
		main int64
		sub  int64
	}{
		{0, 0},
		{1, 1},
		{100, 200},
		{-1, -1},
		{9223372036854775807, 9223372036854775807}, // Max int64
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			bytes := revToBytes(tc.main, tc.sub)
			main, sub := bytesToRev(bytes)
			require.Equal(t, tc.main, main)
			require.Equal(t, tc.sub, sub)
		})
	}
}

func TestRevToBytesFormat(t *testing.T) {
	bytes := revToBytes(100, 200)
	require.Equal(t, 17, len(bytes))
	require.Equal(t, byte('_'), bytes[8])
}

func TestBasicSnapshot(t *testing.T) {
	ds := &DatasourceExecuting{
		path:         "data/basic.snapshot",
		fieldIndices: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
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
				octosql.NewFloat(2),
				octosql.NewFloat(2),
				octosql.NewFloat(1),
				octosql.NewFloat(0),
				octosql.NewString("b"),
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
				octosql.NewFloat(3),
				octosql.NewFloat(3),
				octosql.NewFloat(1),
				octosql.NewFloat(0),
				octosql.NewString("c"),
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
				octosql.NewFloat(4),
				octosql.NewFloat(4),
				octosql.NewFloat(1),
				octosql.NewFloat(0),
				octosql.NewString("e"),
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

func TestBasicSnapshotWithPartialFields(t *testing.T) {
	ds := &DatasourceExecuting{
		path:         "data/basic.snapshot",
		fieldIndices: []int{0, 10, 11}, // Only key, value, and valueSize
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
	require.Equal(t, 3, len(records))

	// Check that each record has only 3 fields
	for _, record := range records {
		require.Equal(t, 3, len(record.Values))
	}
}

func TestDatasourceExecutingWithNonexistentFile(t *testing.T) {
	ds := &DatasourceExecuting{
		path:         "nonexistent.snapshot",
		fieldIndices: []int{0, 1, 2},
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

	require.Error(t, err)
	require.Contains(t, err.Error(), "no such file or directory")
}

func TestDatasourceExecutingProduceError(t *testing.T) {
	ds := &DatasourceExecuting{
		path:         "data/basic.snapshot",
		fieldIndices: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
	}

	expectedErr := fmt.Errorf("produce error")
	err := ds.Run(execution.ExecutionContext{
		Context:         context.TODO(),
		VariableContext: nil,
	},
		func(ctx execution.ProduceContext, record execution.Record) error {
			return expectedErr
		},
		nil,
	)

	require.Error(t, err)
	require.Equal(t, expectedErr, err)
}

func TestDatasourceExecutingWithFieldsOutOfRange(t *testing.T) {
	ds := &DatasourceExecuting{
		path:         "data/basic.snapshot",
		fieldIndices: []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 99}, // 99 is out of range
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
	require.Equal(t, 3, len(records))

	// Check that each record has 12 fields (the out-of-range index is skipped)
	for _, record := range records {
		require.Equal(t, 12, len(record.Values))
	}
}

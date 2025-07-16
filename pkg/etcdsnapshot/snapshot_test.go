package etcdsnapshot

import (
	"context"
	"testing"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/stretchr/testify/require"
)

func TestCreator(t *testing.T) {
	configDecoder := &mockConfigDecoder{}
	db, err := Creator(context.Background(), configDecoder)
	require.NoError(t, err)
	require.NotNil(t, db)
	require.IsType(t, &Database{}, db)
}

func TestDatabaseListTables(t *testing.T) {
	db := &Database{}
	tables, err := db.ListTables(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"etcdsnapshot"}, tables)
}

func TestDatabaseGetTableWithMetaOption(t *testing.T) {
	db := &Database{}
	options := map[string]string{"meta": "true"}

	ds, schema, err := db.GetTable(context.Background(), "test.snapshot", options)
	require.NoError(t, err)
	require.NotNil(t, ds)
	require.NotNil(t, schema)

	// Check that we get the etcdSnapshotDataSource
	etcdDS, ok := ds.(*etcdSnapshotDataSource)
	require.True(t, ok)
	require.Equal(t, "test.snapshot", etcdDS.path)
	require.Equal(t, SchemaMeta, etcdDS.schema)
	require.Equal(t, 24, len(etcdDS.schemaFields))

	// Check first few schema fields for meta
	expectedFields := []struct {
		name string
		typ  octosql.Type
	}{
		{"size", octosql.Int},
		{"sizeInUse", octosql.Int},
		{"sizeFree", octosql.Int},
		{"fragmentationRatio", octosql.Float},
		{"fragmentationBytes", octosql.Int},
		{"totalKeys", octosql.Int},
		{"totalRevisions", octosql.Int},
		{"maxRevision", octosql.Int},
		{"minRevision", octosql.Int},
		{"revisionRange", octosql.Int},
		{"avgRevisionsPerKey", octosql.Float},
		{"defaultQuota", octosql.Int},
		{"quotaUsageRatio", octosql.Float},
		{"quotaUsagePercent", octosql.Float},
		{"quotaRemaining", octosql.Int},
		{"totalValueSize", octosql.Int},
		{"averageValueSize", octosql.Int},
		{"largestValueSize", octosql.Int},
		{"smallestValueSize", octosql.Int},
		{"keysWithMultipleRevisions", octosql.Int},
		{"uniqueKeys", octosql.Int},
		{"keysWithLeases", octosql.Int},
		{"activeLeases", octosql.Int},
		{"estimatedCompactionSavings", octosql.Int},
	}

	for i, expected := range expectedFields {
		require.Equal(t, expected.name, etcdDS.schemaFields[i].Name)
		require.Equal(t, expected.typ, etcdDS.schemaFields[i].Type)
	}
}

func TestDatabaseGetTableWithoutMetaOption(t *testing.T) {
	db := &Database{}
	options := map[string]string{}

	ds, schema, err := db.GetTable(context.Background(), "test.snapshot", options)
	require.NoError(t, err)
	require.NotNil(t, ds)
	require.NotNil(t, schema)

	// Check that we get the etcdSnapshotDataSource
	etcdDS, ok := ds.(*etcdSnapshotDataSource)
	require.True(t, ok)
	require.Equal(t, "test.snapshot", etcdDS.path)
	require.Equal(t, SchemaContent, etcdDS.schema)
	require.Equal(t, 12, len(etcdDS.schemaFields))

	// Check schema fields for content
	expectedFields := []struct {
		name      string
		fieldType octosql.Type
	}{
		{"key", octosql.String},
		{"apiserverPrefix", octosql.TypeSum(octosql.Null, octosql.String)},
		{"apigroup", octosql.TypeSum(octosql.Null, octosql.String)},
		{"resourceType", octosql.TypeSum(octosql.Null, octosql.String)},
		{"namespace", octosql.TypeSum(octosql.Null, octosql.String)},
		{"name", octosql.TypeSum(octosql.Null, octosql.String)},
		{"createRevision", octosql.Int},
		{"modRevision", octosql.Int},
		{"version", octosql.Int},
		{"lease", octosql.Int},
		{"value", octosql.String},
		{"valueSize", octosql.Int},
	}

	for i, field := range etcdDS.schemaFields {
		require.Equal(t, expectedFields[i].name, field.Name)
		require.Equal(t, expectedFields[i].fieldType, field.Type)
	}
}

func TestEtcdSnapshotDataSourceMaterialize(t *testing.T) {
	ds := &etcdSnapshotDataSource{
		path:   "test.snapshot",
		schema: SchemaContent,
		schemaFields: []physical.SchemaField{
			{Name: "key", Type: octosql.String},
			{Name: "value", Type: octosql.String},
			{Name: "valueSize", Type: octosql.Int},
		},
	}

	// Create a schema with subset of fields
	schema := physical.NewSchema([]physical.SchemaField{
		{Name: "key", Type: octosql.String},
		{Name: "valueSize", Type: octosql.Int},
	}, -1)

	node, err := ds.Materialize(context.Background(), physical.Environment{}, schema, nil)
	require.NoError(t, err)
	require.NotNil(t, node)

	// Check that we get the DatasourceExecuting
	dsExec, ok := node.(*DatasourceExecuting)
	require.True(t, ok)
	require.Equal(t, "test.snapshot", dsExec.path)
	require.Equal(t, SchemaContent, dsExec.schema)
	require.Equal(t, []int{0, 2}, dsExec.fieldIndices) // key=0, valueSize=2
}

func TestEtcdSnapshotDataSourceMaterializeWithNoMatchingFields(t *testing.T) {
	ds := &etcdSnapshotDataSource{
		path:   "test.snapshot",
		schema: SchemaContent,
		schemaFields: []physical.SchemaField{
			{Name: "key", Type: octosql.String},
			{Name: "value", Type: octosql.String},
		},
	}

	// Create a schema with non-matching fields
	schema := physical.NewSchema([]physical.SchemaField{
		{Name: "nonexistent", Type: octosql.String},
	}, -1)

	node, err := ds.Materialize(context.Background(), physical.Environment{}, schema, nil)
	require.NoError(t, err)
	require.NotNil(t, node)

	// Check that we get the DatasourceExecuting with empty fieldIndices
	dsExec, ok := node.(*DatasourceExecuting)
	require.True(t, ok)
	require.Equal(t, "test.snapshot", dsExec.path)
	require.Equal(t, SchemaContent, dsExec.schema)
	require.Nil(t, dsExec.fieldIndices)
}

func TestEtcdSnapshotDataSourceMaterializeWithMetaSchema(t *testing.T) {
	ds := &etcdSnapshotDataSource{
		path:   "test.snapshot",
		schema: SchemaMeta,
		schemaFields: []physical.SchemaField{
			{Name: "size", Type: octosql.Int},
			{Name: "sizeInUse", Type: octosql.Int},
			{Name: "sizeFree", Type: octosql.Int},
		},
	}

	// Create a schema with all meta fields
	schema := physical.NewSchema([]physical.SchemaField{
		{Name: "size", Type: octosql.Int},
		{Name: "sizeInUse", Type: octosql.Int},
		{Name: "sizeFree", Type: octosql.Int},
	}, -1)

	node, err := ds.Materialize(context.Background(), physical.Environment{}, schema, nil)
	require.NoError(t, err)
	require.NotNil(t, node)

	// Check that we get the DatasourceExecuting
	dsExec, ok := node.(*DatasourceExecuting)
	require.True(t, ok)
	require.Equal(t, "test.snapshot", dsExec.path)
	require.Equal(t, SchemaMeta, dsExec.schema)
	require.Equal(t, []int{0, 1, 2}, dsExec.fieldIndices)
}

func TestEtcdSnapshotDataSourcePushDownPredicates(t *testing.T) {
	ds := &etcdSnapshotDataSource{
		path:   "test.snapshot",
		schema: SchemaContent,
	}

	// Test predicates (we don't push down any for now)
	newPredicates := []physical.Expression{
		// These would be actual expression objects in real usage
	}
	pushedDownPredicates := []physical.Expression{
		// These would be actual expression objects in real usage
	}

	rejected, pushedDown, changed := ds.PushDownPredicates(newPredicates, pushedDownPredicates)

	require.Equal(t, newPredicates, rejected)
	require.Equal(t, []physical.Expression{}, pushedDown)
	require.False(t, changed)
}

func TestSchemaConstants(t *testing.T) {
	require.Equal(t, Schema(0), SchemaContent)
	require.Equal(t, Schema(1), SchemaMeta)
	require.NotEqual(t, SchemaContent, SchemaMeta)
}

// mockConfigDecoder is a mock implementation of plugins.ConfigDecoder
type mockConfigDecoder struct{}

func (m *mockConfigDecoder) Decode(v interface{}) error {
	// For testing purposes, we'll just return nil
	// In real usage, this would decode the config
	return nil
}

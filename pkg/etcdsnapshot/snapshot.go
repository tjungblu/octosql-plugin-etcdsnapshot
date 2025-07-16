package etcdsnapshot

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins"
)

type Schema int

const (
	SchemaContent Schema = iota
	SchemaMeta    Schema = iota
)

type etcdSnapshotDataSource struct {
	path         string
	schema       Schema
	schemaFields []physical.SchemaField
}

type Config struct {
}

type Database struct {
}

func Creator(ctx context.Context, configUntyped plugins.ConfigDecoder) (physical.Database, error) {
	var cfg Config
	if err := configUntyped.Decode(&cfg); err != nil {
		return nil, err
	}
	return &Database{}, nil
}

func (d Database) ListTables(ctx context.Context) ([]string, error) {
	return []string{"etcdsnapshot"}, nil
}

func (d Database) GetTable(ctx context.Context, name string, options map[string]string) (physical.DatasourceImplementation, physical.Schema, error) {
	if _, ok := options["meta"]; ok {
		schemaFields := []physical.SchemaField{
			// Basic storage info (indices 0-2)
			{
				// size of the entire database file
				Name: "size",
				Type: octosql.Int,
			},
			{
				// how many bytes of "size" are in use
				Name: "sizeInUse",
				Type: octosql.Int,
			},
			{
				// how much space is considered free, meaning "size - sizeInUse".
				Name: "sizeFree",
				Type: octosql.Int,
			},

			// Defragmentation metrics (indices 3-4)
			{
				Name: "fragmentationRatio",
				Type: octosql.Float,
			},
			{
				Name: "fragmentationBytes",
				Type: octosql.Int,
			},

			// Compaction metrics (indices 5-10)
			{
				Name: "totalKeys",
				Type: octosql.Int,
			},
			{
				Name: "totalRevisions",
				Type: octosql.Int,
			},
			{
				Name: "maxRevision",
				Type: octosql.Int,
			},
			{
				Name: "minRevision",
				Type: octosql.Int,
			},
			{
				Name: "revisionRange",
				Type: octosql.Int,
			},
			{
				Name: "avgRevisionsPerKey",
				Type: octosql.Float,
			},

			// Storage quota info (indices 11-14)
			{
				Name: "defaultQuota",
				Type: octosql.Int,
			},
			{
				Name: "quotaUsageRatio",
				Type: octosql.Float,
			},
			{
				Name: "quotaUsagePercent",
				Type: octosql.Float,
			},
			{
				Name: "quotaRemaining",
				Type: octosql.Int,
			},

			// Value size statistics (indices 15-18)
			{
				Name: "totalValueSize",
				Type: octosql.Int,
			},
			{
				Name: "averageValueSize",
				Type: octosql.Int,
			},
			{
				Name: "largestValueSize",
				Type: octosql.Int,
			},
			{
				Name: "smallestValueSize",
				Type: octosql.Int,
			},

			// Key distribution (indices 19-23)
			{
				Name: "keysWithMultipleRevisions",
				Type: octosql.Int,
			},
			{
				Name: "uniqueKeys",
				Type: octosql.Int,
			},
			{
				Name: "keysWithLeases",
				Type: octosql.Int,
			},
			{
				Name: "activeLeases",
				Type: octosql.Int,
			},
			{
				Name: "estimatedCompactionSavings",
				Type: octosql.Int,
			},
		}

		return &etcdSnapshotDataSource{path: name, schemaFields: schemaFields, schema: SchemaMeta},
			physical.NewSchema(schemaFields, -1, physical.WithNoRetractions(true)), nil
	}

	schemaFields := []physical.SchemaField{
		{
			// that's the full key
			Name: "key",
			Type: octosql.String,
		},
		{
			// that's the prefix defined in the apiserver, for example openshift.io, kubernetes.io or registry
			Name: "apiserverPrefix",
			Type: octosql.TypeSum(octosql.Null, octosql.String),
		},
		{
			// that's for example cloudcredential.openshift.io
			Name: "apigroup",
			Type: octosql.TypeSum(octosql.Null, octosql.String),
		},
		{
			// that's a "pod", "service", "deployment"
			Name: "resourceType",
			Type: octosql.TypeSum(octosql.Null, octosql.String),
		},
		{
			Name: "namespace",
			Type: octosql.TypeSum(octosql.Null, octosql.String),
		},
		{
			Name: "name",
			Type: octosql.TypeSum(octosql.Null, octosql.String),
		},
		{
			Name: "createRevision",
			Type: octosql.Int,
		},
		{
			Name: "modRevision",
			Type: octosql.Int,
		},
		{
			Name: "version",
			Type: octosql.Int,
		},
		{
			Name: "lease",
			Type: octosql.Int,
		},
		// this should always be the last entry in this definition listing
		{
			Name: "value",
			Type: octosql.String,
		},
		{
			Name: "valueSize",
			Type: octosql.Int,
		},
	}

	return &etcdSnapshotDataSource{path: name, schemaFields: schemaFields, schema: SchemaContent}, physical.NewSchema(schemaFields, -1, physical.WithNoRetractions(true)), nil

}

func (i *etcdSnapshotDataSource) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (execution.Node, error) {
	fmt.Printf("etcd query predicates %v\n", pushedDownPredicates)
	fmt.Printf("etcd query env %v\n", env)
	fmt.Printf("etcd query schema %v\n", schema)

	var fieldIndices []int
	// this is a silly n^2 loop, but we don't have that many columns for it to matter
	for _, field := range schema.Fields {
		for i, sf := range i.schemaFields {
			if sf.Name == field.Name {
				fieldIndices = append(fieldIndices, i)
				break
			}
		}
	}

	fmt.Printf("etcd query resolved indices %v for schema %d\n", fieldIndices, i.schema)
	return &DatasourceExecuting{
		path:         i.path,
		fieldIndices: fieldIndices,
		schema:       i.schema,
	}, nil
}

func (i *etcdSnapshotDataSource) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

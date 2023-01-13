package etcdsnapshot

import (
	"context"
	"fmt"

	"github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins"
)

type impl struct {
	path         string
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
	}

	return &impl{path: name, schemaFields: schemaFields}, physical.NewSchema(schemaFields, -1, physical.WithNoRetractions(true)), nil
}

func (i *impl) Materialize(ctx context.Context, env physical.Environment, schema physical.Schema, pushedDownPredicates []physical.Expression) (execution.Node, error) {
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

	fmt.Printf("etcd query resolved indices %v\n", fieldIndices)
	return &DatasourceExecuting{
		path:         i.path,
		fieldIndices: fieldIndices,
	}, nil
}

func (i *impl) PushDownPredicates(newPredicates, pushedDownPredicates []physical.Expression) (rejected, pushedDown []physical.Expression, changed bool) {
	return newPredicates, []physical.Expression{}, false
}

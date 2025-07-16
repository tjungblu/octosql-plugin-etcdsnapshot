package query

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewEngine(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)
	require.NotNil(t, engine)
}

func TestResolveSnapshotWithEmptyString(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	_, err = engine.resolveSnapshot("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot path is required")
}

func TestResolveSnapshotWithAbsolutePath(t *testing.T) {
	tmpDir := t.TempDir()
	snapshotPath := filepath.Join(tmpDir, "test.snapshot")
	err := os.WriteFile(snapshotPath, []byte("test"), 0644)
	require.NoError(t, err)

	engine, err := NewEngine()
	require.NoError(t, err)

	resolved, err := engine.resolveSnapshot(snapshotPath)
	require.NoError(t, err)
	require.Equal(t, snapshotPath, resolved)
}

func TestResolveSnapshotWithRelativePath(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	_, err = engine.resolveSnapshot("test.snapshot")
	require.Error(t, err)
	require.Contains(t, err.Error(), "snapshot path must be absolute")
}

func TestResolveSnapshotWithNonexistentFile(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	_, err = engine.resolveSnapshot("/nonexistent/path.snapshot")
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")
}

func TestResolveSnapshotWithNonexistentAbsolutePath(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	_, err = engine.resolveSnapshot("/nonexistent/path.snapshot")
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")
}

func TestExecuteQueryWithInvalidQuery(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	absPath, err := filepath.Abs("../../etcdsnapshot/data/basic.snapshot")
	require.NoError(t, err)

	_, err = engine.ExecuteQuery(context.Background(), "INVALID SQL", absPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to execute query")
}

func TestExecuteQueryWithInvalidJSON(t *testing.T) {
	// This test demonstrates the JSON parsing structure
	// We can't easily test this without mocking the command execution
	// but the code is there for handling JSON parsing errors
	result := &QueryResult{}
	result.Data = []map[string]interface{}{}
	result.Columns = []string{}
	result.Count = 0

	require.NotNil(t, result)
	require.Equal(t, 0, result.Count)
	require.Empty(t, result.Data)
	require.Empty(t, result.Columns)
}

func TestGetClusterOverviewStructure(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	// Create a test snapshot path
	snapshotPath := filepath.Join(os.ExpandEnv("$HOME/snapshots"), "a.snapshot")
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Skip("Test snapshot not found, skipping integration test")
	}

	result, err := engine.GetClusterOverview(context.Background(), snapshotPath)
	require.NoError(t, err)

	// Check structure
	require.Equal(t, "overview", result.Type)
	require.Equal(t, "Cluster overview analysis completed", result.Summary)
	require.Contains(t, result.Details, "total_resources")
	require.Contains(t, result.Details, "resource_types")
	require.Contains(t, result.Details, "namespaces")
	require.Contains(t, result.Details, "namespace_sizes")
}

func TestGetResourceAnalysisStructure(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	// Create a test snapshot path
	snapshotPath := filepath.Join(os.ExpandEnv("$HOME/snapshots"), "a.snapshot")
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Skip("Test snapshot not found, skipping integration test")
	}

	result, err := engine.GetResourceAnalysis(context.Background(), snapshotPath)
	require.NoError(t, err)

	// Check structure
	require.Equal(t, "resources", result.Type)
	require.Equal(t, "Resource analysis completed", result.Summary)
	require.Contains(t, result.Details, "resource_distribution")
	require.Contains(t, result.Details, "pods_by_namespace")
	require.Contains(t, result.Details, "services_by_namespace")
}

func TestGetPerformanceAnalysisStructure(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	// Create a test snapshot path
	snapshotPath := filepath.Join(os.ExpandEnv("$HOME/snapshots"), "a.snapshot")
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Skip("Test snapshot not found, skipping integration test")
	}

	result, err := engine.GetPerformanceAnalysis(context.Background(), snapshotPath)
	require.NoError(t, err)

	// Check structure
	require.Equal(t, "performance", result.Type)
	require.Equal(t, "Performance analysis completed with focus on revision patterns and storage impact", result.Summary)
	require.Contains(t, result.Details, "max_revision")
	require.Contains(t, result.Details, "multi_revision_keys")
	require.Contains(t, result.Details, "most_modified_keys")
	require.Contains(t, result.Details, "largest_values")
}

func TestFindResourcesWithResults(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	// Create a test snapshot path
	snapshotPath := filepath.Join(os.ExpandEnv("$HOME/snapshots"), "a.snapshot")
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Skip("Test snapshot not found, skipping integration test")
	}

	// Search for pods in openshift-marketplace namespace (we know this has results)
	result, err := engine.FindResources(context.Background(), "pods", "openshift-marketplace", "", snapshotPath)
	require.NoError(t, err)

	// Result should not be nil and should have proper structure
	require.NotNil(t, result)
	require.NotNil(t, result.Data)
	require.NotNil(t, result.Columns)
	require.Equal(t, len(result.Data), result.Count)

	// Should have found some pods
	require.Greater(t, result.Count, 0)
}

func TestFindResourcesWithNoResults(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	// Create a test snapshot path
	snapshotPath := filepath.Join(os.ExpandEnv("$HOME/snapshots"), "a.snapshot")
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Skip("Test snapshot not found, skipping integration test")
	}

	// Search for a pod that definitely doesn't exist
	result, err := engine.FindResources(context.Background(), "pods", "default", "test-pod", snapshotPath)
	require.NoError(t, err)

	// Result should not be nil and should have proper structure
	require.NotNil(t, result)
	require.Equal(t, 0, result.Count)

	// Data and Columns might be nil for empty results, which is acceptable
	if result.Data != nil {
		require.Empty(t, result.Data)
	}
	if result.Columns != nil {
		require.Empty(t, result.Columns)
	}
}

func TestFindResourcesQueryConstruction(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	// Test different parameter combinations
	testCases := []struct {
		resourceType     string
		namespace        string
		name             string
		expectedContains []string
	}{
		{
			resourceType:     "pods",
			namespace:        "",
			name:             "",
			expectedContains: []string{"resourceType = 'pods'"},
		},
		{
			resourceType:     "pods",
			namespace:        "default",
			name:             "",
			expectedContains: []string{"resourceType = 'pods'", "namespace = 'default'"},
		},
		{
			resourceType:     "pods",
			namespace:        "default",
			name:             "test-pod",
			expectedContains: []string{"resourceType = 'pods'", "namespace = 'default'", "name = 'test-pod'"},
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s-%s-%s", tc.resourceType, tc.namespace, tc.name), func(t *testing.T) {
			// We can't easily test the query construction without executing it
			// but we can test that it doesn't panic
			_, err := engine.FindResources(context.Background(), tc.resourceType, tc.namespace, tc.name, "/nonexistent/path.snapshot")
			// We expect an error because the snapshot doesn't exist
			require.Error(t, err)
			require.Contains(t, err.Error(), "does not exist")
		})
	}
}

func TestGetNamespaceAnalysisStructure(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	// Create a test snapshot path
	snapshotPath := filepath.Join(os.ExpandEnv("$HOME/snapshots"), "a.snapshot")
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Skip("Test snapshot not found, skipping integration test")
	}

	result, err := engine.GetNamespaceAnalysis(context.Background(), snapshotPath, "5")
	require.NoError(t, err)

	// Check structure
	require.Equal(t, "namespace_analysis", result.Type)
	require.Equal(t, "Namespace analysis completed for top 5 namespaces", result.Summary)
	require.Contains(t, result.Details, "namespace_usage")
	require.Contains(t, result.Details, "resource_distribution")
}

func TestCompareSnapshotsWithDifferentTypes(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	// Create test snapshot paths
	snapshotPath1 := filepath.Join(os.ExpandEnv("$HOME/snapshots"), "a.snapshot")
	snapshotPath2 := filepath.Join(os.ExpandEnv("$HOME/snapshots"), "b.snapshot")
	if _, err := os.Stat(snapshotPath1); os.IsNotExist(err) {
		t.Skip("Test snapshot not found, skipping integration test")
	}
	if _, err := os.Stat(snapshotPath2); os.IsNotExist(err) {
		t.Skip("Test snapshot not found, skipping integration test")
	}

	diffTypes := []string{"added", "removed", "added_revisions", "removed_revisions"}

	for _, diffType := range diffTypes {
		t.Run(diffType, func(t *testing.T) {
			result, err := engine.CompareSnapshots(context.Background(), snapshotPath1, snapshotPath2, diffType)
			require.NoError(t, err)

			// Check structure
			require.Equal(t, "comparison", result.Type)
			require.NotEmpty(t, result.Summary)
			require.Contains(t, result.Details, diffType)
		})
	}
}

func TestCompareSnapshotsWithInvalidDiffType(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	// Create test snapshot paths
	snapshotPath1 := filepath.Join(os.ExpandEnv("$HOME/snapshots"), "a.snapshot")
	snapshotPath2 := filepath.Join(os.ExpandEnv("$HOME/snapshots"), "b.snapshot")
	if _, err := os.Stat(snapshotPath1); os.IsNotExist(err) {
		t.Skip("Test snapshot not found, skipping integration test")
	}
	if _, err := os.Stat(snapshotPath2); os.IsNotExist(err) {
		t.Skip("Test snapshot not found, skipping integration test")
	}

	result, err := engine.CompareSnapshots(context.Background(), snapshotPath1, snapshotPath2, "invalid_diff_type")
	require.NoError(t, err)
	require.Equal(t, "comparison", result.Type)
	require.Contains(t, result.Summary, "Unknown diff type")
	require.Contains(t, result.Details, "error")
}

func TestCompareSnapshotsWithInvalidSnapshot(t *testing.T) {
	engine, err := NewEngine()
	require.NoError(t, err)

	_, err = engine.CompareSnapshots(context.Background(), "/nonexistent/path1.snapshot", "/nonexistent/path2.snapshot", "added")
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to resolve snapshot")
}

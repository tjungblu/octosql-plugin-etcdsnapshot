package query

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Engine wraps the octosql plugin functionality
type Engine struct {
	snapshotDir string
}

// QueryResult represents the result of a query
type QueryResult struct {
	Data    []map[string]interface{} `json:"data"`
	Columns []string                 `json:"columns"`
	Count   int                      `json:"count"`
}

// AnalysisResult represents the result of an analysis
type AnalysisResult struct {
	Type     string                 `json:"type"`
	Summary  string                 `json:"summary"`
	Details  map[string]interface{} `json:"details"`
	Insights []string               `json:"insights"`
}

// NewEngine creates a new query engine
func NewEngine(snapshotDir string) (*Engine, error) {
	// Ensure snapshot directory exists
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	return &Engine{
		snapshotDir: snapshotDir,
	}, nil
}

// ExecuteQuery executes a SQL query against an etcd snapshot
func (e *Engine) ExecuteQuery(ctx context.Context, query string, snapshot string) (*QueryResult, error) {
	snapshotPath, err := e.resolveSnapshot(snapshot)
	if err != nil {
		return nil, err
	}

	// Replace placeholder with actual snapshot path
	updatedQuery := strings.ReplaceAll(query, "{{SNAPSHOT}}", snapshotPath)

	// Execute octosql query
	cmd := exec.CommandContext(ctx, "octosql", updatedQuery, "--output", "json")
	output, err := cmd.CombinedOutput()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("failed to execute query: exit code %d, query: %s, output: %s", exitErr.ExitCode(), updatedQuery, string(output))
		}
		return nil, fmt.Errorf("failed to execute query: %w, query: %s, output: %s", err, updatedQuery, string(output))
	}

	// Parse newline-delimited JSON output
	var result QueryResult
	var data []map[string]interface{}
	var columns []string               // Preserve order of columns as they first appear
	columnSet := make(map[string]bool) // Track which columns we've seen to avoid duplicates

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")

	for _, line := range lines {
		if line == "" {
			continue
		}

		var row map[string]interface{}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, fmt.Errorf("failed to parse JSON output from octosql: %w (raw output: %s)", err, string(output))
		}

		// Collect column names from this row, preserving order
		for col := range row {
			if !columnSet[col] {
				columns = append(columns, col)
				columnSet[col] = true
			}
		}

		data = append(data, row)
	}

	result = QueryResult{
		Data:    data,
		Columns: columns,
		Count:   len(data),
	}

	return &result, nil
}

// GetClusterOverview provides a high-level cluster overview
func (e *Engine) GetClusterOverview(ctx context.Context, snapshot string) (*AnalysisResult, error) {
	queries := []string{
		"SELECT COUNT(*) as total_resources FROM {{SNAPSHOT}}",
		"SELECT resourceType, COUNT(*) as count FROM {{SNAPSHOT}} GROUP BY resourceType ORDER BY count DESC LIMIT 10",
		"SELECT namespace, COUNT(*) as count FROM {{SNAPSHOT}} WHERE namespace IS NOT NULL GROUP BY namespace ORDER BY count DESC LIMIT 10",
		"SELECT namespace, SUM(valueSize) as total_size FROM {{SNAPSHOT}} WHERE namespace IS NOT NULL GROUP BY namespace ORDER BY total_size DESC LIMIT 5",
	}

	details := make(map[string]interface{})
	insights := []string{}

	for i, query := range queries {
		result, err := e.ExecuteQuery(ctx, query, snapshot)
		if err != nil {
			return nil, fmt.Errorf("failed to execute overview query %d: %w", i, err)
		}

		switch i {
		case 0:
			details["total_resources"] = result.Data
		case 1:
			details["resource_types"] = result.Data
			if len(result.Data) > 0 {
				if count, ok := result.Data[0]["count"].(float64); ok && count > 1000 {
					insights = append(insights, fmt.Sprintf("High resource count detected: %.0f total resources", count))
				}
			}
		case 2:
			details["namespaces"] = result.Data
		case 3:
			details["namespace_sizes"] = result.Data
			if len(result.Data) > 0 {
				if size, ok := result.Data[0]["total_size"].(float64); ok && size > 10000000 {
					insights = append(insights, fmt.Sprintf("Large namespace detected: %.2f MB", size/1000000))
				}
			}
		}
	}

	return &AnalysisResult{
		Type:     "overview",
		Summary:  "Cluster overview analysis completed",
		Details:  details,
		Insights: insights,
	}, nil
}

// GetResourceAnalysis performs resource analysis
func (e *Engine) GetResourceAnalysis(ctx context.Context, snapshot string) (*AnalysisResult, error) {
	queries := []string{
		"SELECT resourceType, COUNT(*) as count FROM {{SNAPSHOT}} GROUP BY resourceType ORDER BY count DESC",
		"SELECT namespace, COUNT(*) as count FROM {{SNAPSHOT}} WHERE resourceType = 'pods' GROUP BY namespace ORDER BY count DESC LIMIT 10",
		"SELECT namespace, COUNT(*) as count FROM {{SNAPSHOT}} WHERE resourceType = 'services' GROUP BY namespace ORDER BY count DESC LIMIT 10",
	}

	details := make(map[string]interface{})
	insights := []string{}

	for i, query := range queries {
		result, err := e.ExecuteQuery(ctx, query, snapshot)
		if err != nil {
			return nil, fmt.Errorf("failed to execute resource query %d: %w", i, err)
		}

		switch i {
		case 0:
			details["resource_distribution"] = result.Data
		case 1:
			details["pods_by_namespace"] = result.Data
		case 2:
			details["services_by_namespace"] = result.Data
		}
	}

	insights = append(insights, "Resource analysis shows cluster resource distribution")

	return &AnalysisResult{
		Type:     "resources",
		Summary:  "Resource analysis completed",
		Details:  details,
		Insights: insights,
	}, nil
}

// GetPerformanceAnalysis performs performance analysis
func (e *Engine) GetPerformanceAnalysis(ctx context.Context, snapshot string) (*AnalysisResult, error) {
	details := make(map[string]interface{})
	insights := []string{}

	// Get maximum revision
	maxRevisionResult, err := e.ExecuteQuery(ctx, "SELECT MAX(createRevision) as max_revision FROM {{SNAPSHOT}} t", snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to execute max revision query: %w", err)
	}
	details["max_revision"] = maxRevisionResult.Data

	// Find keys with multiple revisions and their total impact
	multiRevisionKeysResult, err := e.ExecuteQuery(ctx,
		"SELECT t.key, COUNT(*) as revision_count, SUM(valueSize) as total_size, AVG(valueSize) as avg_size FROM {{SNAPSHOT}} t GROUP BY t.key ORDER BY total_size DESC LIMIT 10",
		snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to execute multi-revision keys query: %w", err)
	}
	details["multi_revision_keys"] = multiRevisionKeysResult.Data

	// Generate insights for high-churn keys
	for _, row := range multiRevisionKeysResult.Data {
		if revCount, ok := row["revision_count"].(float64); ok && revCount > 5 {
			if totalSize, ok := row["total_size"].(float64); ok && totalSize > 100000 {
				if key, ok := row["key"].(string); ok {
					insights = append(insights, fmt.Sprintf("High-churn key detected: '%s' has %.0f revisions totaling %.2f KB", key, revCount, totalSize/1024))
				}
			}
		}
	}

	// Find the most frequently modified keys
	mostModifiedKeysResult, err := e.ExecuteQuery(ctx,
		"SELECT t.key, COUNT(*) as revision_count, MIN(createRevision) as first_revision, MAX(modRevision) as last_revision FROM {{SNAPSHOT}} t GROUP BY t.key ORDER BY revision_count DESC LIMIT 10",
		snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to execute most modified keys query: %w", err)
	}
	details["most_modified_keys"] = mostModifiedKeysResult.Data

	// Generate insights for excessive modifications
	if len(mostModifiedKeysResult.Data) > 0 {
		if count, ok := mostModifiedKeysResult.Data[0]["revision_count"].(float64); ok && count > 10 {
			insights = append(insights, fmt.Sprintf("Excessive key modifications detected: %.0f revisions for top key", count))
		}
	}

	// Find the largest single values (potential bloat)
	largestValuesResult, err := e.ExecuteQuery(ctx,
		"SELECT t.key, valueSize, modRevision FROM {{SNAPSHOT}} t ORDER BY valueSize DESC LIMIT 10",
		snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to execute largest values query: %w", err)
	}
	details["largest_values"] = largestValuesResult.Data

	// Generate insights for large values
	if len(largestValuesResult.Data) > 0 {
		if size, ok := largestValuesResult.Data[0]["valueSize"].(float64); ok && size > 1000000 {
			insights = append(insights, fmt.Sprintf("Large value detected: %.2f MB", size/1000000))
		}
	}

	// Add summary insights
	if len(details["multi_revision_keys"].([]map[string]interface{})) > 0 {
		insights = append(insights, "Multiple keys have multiple revisions - consider investigating write patterns")
	}

	return &AnalysisResult{
		Type:     "performance",
		Summary:  "Performance analysis completed with focus on revision patterns and storage impact",
		Details:  details,
		Insights: insights,
	}, nil
}

// FindResources finds specific resources
func (e *Engine) FindResources(ctx context.Context, resourceType, namespace, name, snapshot string) (*QueryResult, error) {
	query := fmt.Sprintf("SELECT * FROM {{SNAPSHOT}} WHERE resourceType = '%s'", resourceType)

	if namespace != "" {
		query += fmt.Sprintf(" AND namespace = '%s'", namespace)
	}

	if name != "" {
		query += fmt.Sprintf(" AND name = '%s'", name)
	}

	query += " ORDER BY createRevision DESC"

	return e.ExecuteQuery(ctx, query, snapshot)
}

// CompareSnapshots compares two snapshots
func (e *Engine) CompareSnapshots(ctx context.Context, snapshot1, snapshot2, diffType string) (*AnalysisResult, error) {
	snapshot1Path, err := e.resolveSnapshot(snapshot1)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve snapshot1: %w", err)
	}

	snapshot2Path, err := e.resolveSnapshot(snapshot2)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve snapshot2: %w", err)
	}

	switch diffType {
	case "added":
		result, err := e.diffKeys(ctx, snapshot2Path, snapshot1Path)
		if err != nil {
			return nil, fmt.Errorf("failed to find added keys: %w", err)
		}
		return &AnalysisResult{
			Type:     "comparison",
			Summary:  fmt.Sprintf("Found %d keys added in %s", result.Count, snapshot2),
			Details:  map[string]interface{}{"added": result.Data},
			Insights: []string{fmt.Sprintf("Found %d keys added between snapshots", result.Count)},
		}, nil

	case "removed":
		result, err := e.diffKeys(ctx, snapshot1Path, snapshot2Path)
		if err != nil {
			return nil, fmt.Errorf("failed to find removed keys: %w", err)
		}
		return &AnalysisResult{
			Type:     "comparison",
			Summary:  fmt.Sprintf("Found %d keys removed from %s", result.Count, snapshot1),
			Details:  map[string]interface{}{"removed": result.Data},
			Insights: []string{fmt.Sprintf("Found %d keys removed between snapshots", result.Count)},
		}, nil

	case "added_revisions":
		result, err := e.diff(ctx, snapshot2Path, snapshot1Path)
		if err != nil {
			return nil, fmt.Errorf("failed to find added revisions: %w", err)
		}
		return &AnalysisResult{
			Type:     "comparison",
			Summary:  fmt.Sprintf("Found %d revision tuples added in %s", result.Count, snapshot2),
			Details:  map[string]interface{}{"added_revisions": result.Data},
			Insights: []string{fmt.Sprintf("Found %d revision tuples added between snapshots (includes updates to existing keys)", result.Count)},
		}, nil

	case "removed_revisions":
		result, err := e.diff(ctx, snapshot1Path, snapshot2Path)
		if err != nil {
			return nil, fmt.Errorf("failed to find removed revisions: %w", err)
		}
		return &AnalysisResult{
			Type:     "comparison",
			Summary:  fmt.Sprintf("Found %d revision tuples removed from %s", result.Count, snapshot1),
			Details:  map[string]interface{}{"removed_revisions": result.Data},
			Insights: []string{fmt.Sprintf("Found %d revision tuples removed between snapshots (includes updates to existing keys)", result.Count)},
		}, nil

	default:
		return &AnalysisResult{
			Type:     "comparison",
			Summary:  fmt.Sprintf("Unknown diff type: %s", diffType),
			Details:  map[string]interface{}{"error": "Supported diff types: added, removed, added_revisions, removed_revisions"},
			Insights: []string{"Supported diff types: 'added', 'removed' (key-level), 'added_revisions', 'removed_revisions' (revision-level)"},
		}, nil
	}
}

// diff finds (key, revision) tuples that exist in sourceSnapshot but not in targetSnapshot
func (e *Engine) diff(ctx context.Context, sourceSnapshot, targetSnapshot string) (*QueryResult, error) {
	query := fmt.Sprintf(`
		SELECT s2.key, s2.createRevision, s2.modRevision
		FROM %s s2 
		LEFT JOIN %s s1 ON s1.key = s2.key 
			AND s1.createRevision = s2.createRevision 
			AND s1.modRevision = s2.modRevision
		WHERE s1.key IS NULL
		ORDER BY s2.key, s2.modRevision
	`, sourceSnapshot, targetSnapshot)

	return e.ExecuteQuery(ctx, query, "")
}

// diffKeys finds keys that exist in sourceSnapshot but not in targetSnapshot (ignoring revisions)
func (e *Engine) diffKeys(ctx context.Context, sourceSnapshot, targetSnapshot string) (*QueryResult, error) {
	query := fmt.Sprintf(`
		SELECT s1.key, s1.createRevision, s1.modRevision
		FROM %s s1 
		LEFT JOIN %s s2 ON s1.key = s2.key 
		WHERE s2.key IS NULL
		ORDER BY s1.key
	`, sourceSnapshot, targetSnapshot)

	return e.ExecuteQuery(ctx, query, "")
}

// GetNamespaceAnalysis analyzes namespace usage patterns
func (e *Engine) GetNamespaceAnalysis(ctx context.Context, snapshot string, limit string) (*AnalysisResult, error) {
	// Query for namespace storage usage
	query := fmt.Sprintf(`
		SELECT namespace, COUNT(*) as object_count, SUM(valueSize) as total_size_bytes, AVG(valueSize) as avg_size_bytes
		FROM {{SNAPSHOT}} t 
		WHERE namespace IS NOT NULL 
		GROUP BY namespace 
		ORDER BY total_size_bytes DESC 
		LIMIT %s`, limit)

	result, err := e.ExecuteQuery(ctx, query, snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to execute namespace analysis query: %w", err)
	}

	// Query for resource type distribution in top namespaces
	resourceQuery := `
		SELECT namespace, resourceType, COUNT(*) as count, SUM(valueSize) as total_size
		FROM {{SNAPSHOT}} t 
		WHERE namespace IS NOT NULL 
		GROUP BY namespace, resourceType 
		ORDER BY namespace, total_size DESC`

	resourceResult, err := e.ExecuteQuery(ctx, resourceQuery, snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to execute resource distribution query: %w", err)
	}

	details := make(map[string]interface{})
	insights := []string{}

	// Process namespace usage data
	if len(result.Data) > 0 {
		details["namespace_usage"] = result.Data

		// Generate insights
		if totalSize, ok := result.Data[0]["total_size_bytes"].(float64); ok {
			if totalSize > 100*1024*1024 { // > 100MB
				if namespace, ok := result.Data[0]["namespace"].(string); ok {
					insights = append(insights, fmt.Sprintf("Namespace '%s' consumes %.2f MB of etcd storage", namespace, totalSize/(1024*1024)))
				}
			}
		}

		// Check for high object counts
		if objectCount, ok := result.Data[0]["object_count"].(float64); ok {
			if objectCount > 1000 {
				if namespace, ok := result.Data[0]["namespace"].(string); ok {
					insights = append(insights, fmt.Sprintf("Namespace '%s' has %.0f objects - consider monitoring for resource bloat", namespace, objectCount))
				}
			}
		}
	}

	// Process resource distribution
	if len(resourceResult.Data) > 0 {
		details["resource_distribution"] = resourceResult.Data

		// Check for resource type concentrations
		resourceCounts := make(map[string]int)
		for _, row := range resourceResult.Data {
			if resourceType, ok := row["resourceType"].(string); ok {
				resourceCounts[resourceType]++
			}
		}

		for resourceType, count := range resourceCounts {
			if count > 5 {
				insights = append(insights, fmt.Sprintf("Resource type '%s' appears in %d namespaces", resourceType, count))
			}
		}
	}

	return &AnalysisResult{
		Type:     "namespace_analysis",
		Summary:  fmt.Sprintf("Namespace analysis completed for top %s namespaces", limit),
		Details:  details,
		Insights: insights,
	}, nil
}

// resolveSnapshot resolves the snapshot path
func (e *Engine) resolveSnapshot(snapshot string) (string, error) {
	if snapshot == "" {
		// Find the latest snapshot
		globPattern := filepath.Join(e.snapshotDir, "*.snapshot")
		files, err := filepath.Glob(globPattern)
		if err != nil {
			return "", fmt.Errorf("failed to find snapshots using pattern '%s': %w", globPattern, err)
		}

		// Get current working directory for debugging
		cwd, _ := os.Getwd()

		// Check if snapshot directory exists
		if _, err := os.Stat(e.snapshotDir); os.IsNotExist(err) {
			return "", fmt.Errorf("snapshot directory '%s' does not exist (cwd: %s)", e.snapshotDir, cwd)
		}

		// List all files in snapshot directory for debugging
		entries, _ := os.ReadDir(e.snapshotDir)
		var allFiles []string
		for _, entry := range entries {
			allFiles = append(allFiles, entry.Name())
		}

		if len(files) == 0 {
			return "", fmt.Errorf("no snapshots found in '%s' (cwd: %s, pattern: %s, all files: %v)",
				e.snapshotDir, cwd, globPattern, allFiles)
		}

		latest := files[0]
		var latestTime int64
		for _, file := range files {
			info, err := os.Stat(file)
			if err != nil {
				continue
			}

			if info.ModTime().Unix() > latestTime {
				latestTime = info.ModTime().Unix()
				latest = file
			}
		}

		return latest, nil
	}

	// If it's already an absolute path, use it
	if filepath.IsAbs(snapshot) {
		// Check if absolute path exists
		if _, err := os.Stat(snapshot); os.IsNotExist(err) {
			cwd, _ := os.Getwd()
			return "", fmt.Errorf("absolute snapshot path '%s' does not exist (cwd: %s)", snapshot, cwd)
		}
		return snapshot, nil
	}

	// Otherwise, resolve relative to snapshot directory
	resolvedPath := filepath.Join(e.snapshotDir, snapshot)

	// Check if resolved path exists
	if _, err := os.Stat(resolvedPath); os.IsNotExist(err) {
		cwd, _ := os.Getwd()
		return "", fmt.Errorf("resolved snapshot path '%s' does not exist (snapshot: %s, snapshotDir: %s, cwd: %s)",
			resolvedPath, snapshot, e.snapshotDir, cwd)
	}

	return resolvedPath, nil
}

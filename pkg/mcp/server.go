package mcp

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/tjungblu/octosql-plugin-etcdsnapshot/pkg/query"
)

// Config holds the MCP server configuration
type Config struct {
	Name        string
	Version     string
	Description string
}

// Server represents the MCP server
type Server struct {
	config      Config
	queryEngine *query.Engine
	mcpServer   *server.MCPServer
}

// NewServer creates a new MCP server
func NewServer(config Config) (*Server, error) {
	// Initialize query engine
	queryEngine, err := query.NewEngine()
	if err != nil {
		return nil, fmt.Errorf("failed to create query engine: %w", err)
	}

	// Create MCP server with tools capability
	mcpServer := server.NewMCPServer(
		config.Name,
		config.Version,
		server.WithToolCapabilities(true),
	)

	s := &Server{
		config:      config,
		queryEngine: queryEngine,
		mcpServer:   mcpServer,
	}

	// Register our tools
	s.registerTools()

	return s, nil
}

// Start starts the MCP server
func (s *Server) Start(ctx context.Context) error {
	// Start the server using stdio transport
	return server.ServeStdio(s.mcpServer)
}

func (s *Server) registerTools() {
	// Register query_etcd tool
	queryTool := mcp.NewTool("query_etcd",
		mcp.WithDescription("Execute SQL queries against etcd snapshots. Use standard SQL syntax with table alias 't' for the snapshot (e.g., 'SELECT t.key, valueSize FROM {{SNAPSHOT}} t WHERE t.key LIKE \"/kubernetes.io/pods/%\" LIMIT 10'). The schema includes: key, namespace, resourceType, name, value, valueSize, createRevision, modRevision, version, lease, apigroup, apiserverPrefix."),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("SQL query to execute. Use {{SNAPSHOT}} as placeholder for snapshot path and table alias 't' for queries (e.g., 'SELECT namespace, COUNT(*) FROM {{SNAPSHOT}} t GROUP BY namespace ORDER BY COUNT(*) DESC LIMIT 5'). Note: 'key' is reserved, use 't.key' instead."),
		),
		mcp.WithString("snapshot",
			mcp.Required(),
			mcp.Description("Absolute path to the snapshot file to query (e.g., '/path/to/snapshot.db'). Relative paths are not supported."),
		),
	)

	s.mcpServer.AddTool(queryTool, s.handleQueryEtcd)

	// Register analyze_cluster tool
	analyzeTool := mcp.NewTool("analyze_cluster",
		mcp.WithDescription("Perform comprehensive cluster analysis. 'overview' provides general cluster health and resource counts; 'resources' shows resource distribution by namespace and type; 'performance' identifies high-churn keys, largest values, and performance hotspots in etcd."),
		mcp.WithString("analysis_type",
			mcp.Required(),
			mcp.Description("Type of analysis: 'overview' (cluster summary and health), 'resources' (resource breakdown by namespace/type), 'performance' (revision patterns, storage impact, hotspots)"),
			mcp.Enum("overview", "resources", "performance"),
		),
		mcp.WithString("snapshot",
			mcp.Required(),
			mcp.Description("Absolute path to the snapshot file to analyze (e.g., '/path/to/snapshot.db'). Relative paths are not supported."),
		),
	)

	s.mcpServer.AddTool(analyzeTool, s.handleAnalyzeCluster)

	// Register find_resources tool
	findTool := mcp.NewTool("find_resources",
		mcp.WithDescription("Find specific Kubernetes resources by type, namespace, or name. Useful for locating specific objects or getting resource counts."),
		mcp.WithString("resource_type",
			mcp.Required(),
			mcp.Description("Kubernetes resource type to find. Examples: 'pods', 'services', 'deployments', 'configmaps', 'secrets', 'events', 'applicationsets', 'applications', 'clusterroles', 'namespaces', 'nodes', 'persistentvolumes', etc."),
		),
		mcp.WithString("namespace",
			mcp.Description("Namespace to search in (optional). Examples: 'default', 'kube-system', 'openshift-gitops', 'openshift-monitoring'"),
		),
		mcp.WithString("name",
			mcp.Description("Resource name to search for (optional). Can be exact name or partial match."),
		),
		mcp.WithString("snapshot",
			mcp.Required(),
			mcp.Description("Absolute path to the snapshot file to search in (e.g., '/path/to/snapshot.db'). Relative paths are not supported."),
		),
	)

	s.mcpServer.AddTool(findTool, s.handleFindResources)

	// Register compare_snapshots tool
	compareTool := mcp.NewTool("compare_snapshots",
		mcp.WithDescription("Compare two etcd snapshots to identify differences over time. Useful for change tracking, debugging, and understanding cluster evolution."),
		mcp.WithString("snapshot1",
			mcp.Required(),
			mcp.Description("Absolute path to the first snapshot file (baseline/older snapshot) (e.g., '/path/to/snapshot1.db'). Relative paths are not supported."),
		),
		mcp.WithString("snapshot2",
			mcp.Required(),
			mcp.Description("Absolute path to the second snapshot file (comparison/newer snapshot) (e.g., '/path/to/snapshot2.db'). Relative paths are not supported."),
		),
		mcp.WithString("diff_type",
			mcp.Description("Type of changes to show: 'added' (new keys), 'removed' (deleted keys), 'added_revisions' (new revision tuples), 'removed_revisions' (deleted revision tuples)"),
			mcp.Enum("added", "removed", "added_revisions", "removed_revisions"),
			mcp.DefaultString("added"),
		),
	)

	s.mcpServer.AddTool(compareTool, s.handleCompareSnapshots)

	// Register namespace_analysis tool
	namespaceTool := mcp.NewTool("analyze_namespaces",
		mcp.WithDescription("Analyze namespace usage patterns including storage consumption, object counts, and resource distribution. Provides insights into which namespaces are using the most etcd storage."),
		mcp.WithString("snapshot",
			mcp.Required(),
			mcp.Description("Absolute path to the snapshot file to analyze (e.g., '/path/to/snapshot.db'). Relative paths are not supported."),
		),
		mcp.WithString("limit",
			mcp.Description("Number of top namespaces to return (default: 10)"),
			mcp.DefaultString("10"),
		),
	)

	s.mcpServer.AddTool(namespaceTool, s.handleNamespaceAnalysis)

	// Register get_snapshot_metadata tool
	metadataTool := mcp.NewTool("get_snapshot_metadata",
		mcp.WithDescription("Get comprehensive metadata about an etcd snapshot including storage statistics, fragmentation metrics, compaction info, quota usage, and key distribution. This provides deep insights into the snapshot's storage characteristics and health."),
		mcp.WithString("snapshot",
			mcp.Required(),
			mcp.Description("Absolute path to the snapshot file to analyze (e.g., '/path/to/snapshot.db'). Relative paths are not supported."),
		),
	)

	s.mcpServer.AddTool(metadataTool, s.handleGetSnapshotMetadata)

	// Register analyze_storage_health tool
	healthTool := mcp.NewTool("analyze_storage_health",
		mcp.WithDescription("Analyze storage health using snapshot metadata including fragmentation analysis, quota usage assessment, compaction efficiency, and recommendations for optimization."),
		mcp.WithString("snapshot",
			mcp.Required(),
			mcp.Description("Absolute path to the snapshot file to analyze (e.g., '/path/to/snapshot.db'). Relative paths are not supported."),
		),
	)

	s.mcpServer.AddTool(healthTool, s.handleAnalyzeStorageHealth)
}

func (s *Server) handleQueryEtcd(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	query, err := request.RequireString("query")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	snapshot, err := request.RequireString("snapshot")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	result, err := s.queryEngine.ExecuteQuery(ctx, query, snapshot)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Query execution failed: %v", err)), nil
	}

	return mcp.NewToolResultText(fmt.Sprintf("Query executed successfully. Results:\n%+v", result)), nil
}

func (s *Server) handleAnalyzeCluster(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	analysisType, err := request.RequireString("analysis_type")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	snapshot, err := request.RequireString("snapshot")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	var result interface{}
	switch analysisType {
	case "overview":
		result, err = s.queryEngine.GetClusterOverview(ctx, snapshot)
	case "resources":
		result, err = s.queryEngine.GetResourceAnalysis(ctx, snapshot)
	case "performance":
		result, err = s.queryEngine.GetPerformanceAnalysis(ctx, snapshot)
	default:
		return mcp.NewToolResultError(fmt.Sprintf("Unsupported analysis type: %s", analysisType)), nil
	}

	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Analysis failed: %v", err)), nil
	}

	return mcp.NewToolResultText(fmt.Sprintf("Cluster analysis (%s) completed successfully:\n%+v", analysisType, result)), nil
}

func (s *Server) handleFindResources(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	resourceType, err := request.RequireString("resource_type")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	namespace := request.GetString("namespace", "")
	name := request.GetString("name", "")
	snapshot, err := request.RequireString("snapshot")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	result, err := s.queryEngine.FindResources(ctx, resourceType, namespace, name, snapshot)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Resource search failed: %v", err)), nil
	}

	return mcp.NewToolResultText(fmt.Sprintf("Found %d resources of type '%s':\n%+v", len(result.Data), resourceType, result)), nil
}

func (s *Server) handleCompareSnapshots(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snapshot1, err := request.RequireString("snapshot1")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	snapshot2, err := request.RequireString("snapshot2")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	diffType := request.GetString("diff_type", "added")

	result, err := s.queryEngine.CompareSnapshots(ctx, snapshot1, snapshot2, diffType)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Snapshot comparison failed: %v", err)), nil
	}

	return mcp.NewToolResultText(fmt.Sprintf("Snapshot comparison (%s) completed successfully:\n%+v", diffType, result)), nil
}

func (s *Server) handleNamespaceAnalysis(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snapshot, err := request.RequireString("snapshot")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	limit := request.GetString("limit", "10")

	result, err := s.queryEngine.GetNamespaceAnalysis(ctx, snapshot, limit)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Namespace analysis failed: %v", err)), nil
	}

	return mcp.NewToolResultText(fmt.Sprintf("Namespace analysis completed successfully:\n%+v", result)), nil
}

func (s *Server) handleGetSnapshotMetadata(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snapshot, err := request.RequireString("snapshot")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	result, err := s.queryEngine.GetSnapshotMetadata(ctx, snapshot)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Snapshot metadata retrieval failed: %v", err)), nil
	}

	return mcp.NewToolResultText(fmt.Sprintf("Snapshot metadata retrieved successfully:\n%+v", result)), nil
}

func (s *Server) handleAnalyzeStorageHealth(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	snapshot, err := request.RequireString("snapshot")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	result, err := s.queryEngine.AnalyzeStorageHealth(ctx, snapshot)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Storage health analysis failed: %v", err)), nil
	}

	return mcp.NewToolResultText(fmt.Sprintf("Storage health analysis completed successfully:\n%+v", result)), nil
}

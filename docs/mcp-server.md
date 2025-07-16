# MCP Server for etcd Snapshot Analysis

This MCP (Model Context Protocol) server allows AI assistants to analyze Kubernetes/OpenShift cluster state by querying etcd snapshots using natural language.

## Overview

The MCP server exposes powerful cluster analysis capabilities through a set of tools that AI assistants can use to:

- Execute custom SQL queries against etcd snapshots
- Perform high-level cluster analysis
- Find specific Kubernetes resources
- Compare snapshots to detect changes
- Analyze comprehensive storage health and performance metrics
- Get detailed metadata about etcd snapshots including fragmentation, quota usage, and compaction efficiency

## Available Tools

### 1. `query_etcd`
Execute custom SQL queries against etcd snapshots.

**Parameters:**
- `query` (required): SQL query to execute
- `snapshot` (required): Absolute path to the snapshot file (e.g., `/path/to/snapshot.db`)

**Example:**
```json
{
  "query": "SELECT namespace, COUNT(*) as count FROM {{SNAPSHOT}} t WHERE t.resourceType = 'pods' GROUP BY namespace ORDER BY count DESC LIMIT 10",
  "snapshot": "/home/user/snapshots/cluster-snapshot-2024-01-15.snapshot"
}
```

### 2. `analyze_cluster`
Perform high-level cluster analysis.

**Parameters:**
- `analysis_type` (required): Type of analysis:
  - `overview`: General cluster health and resource counts with metadata insights
  - `resources`: Resource distribution by namespace and type with storage metrics
  - `performance`: Revision patterns, storage impact, and performance hotspots
- `snapshot` (required): Absolute path to the snapshot file to analyze

**Example:**
```json
{
  "analysis_type": "performance",
  "snapshot": "/home/user/snapshots/prod-cluster.snapshot"
}
```

### 3. `find_resources`
Find specific Kubernetes resources.

**Parameters:**
- `resource_type` (required): Type of resource (e.g., `pods`, `services`, `deployments`)
- `namespace` (optional): Namespace to search in
- `name` (optional): Resource name to search for
- `snapshot` (required): Absolute path to the snapshot file to search in

**Example:**
```json
{
  "resource_type": "deployments",
  "namespace": "kube-system",
  "name": "coredns",
  "snapshot": "/home/user/snapshots/cluster.snapshot"
}
```

### 4. `compare_snapshots`
Compare two etcd snapshots to find differences.

**Parameters:**
- `snapshot1` (required): Absolute path to the first snapshot file
- `snapshot2` (required): Absolute path to the second snapshot file
- `diff_type` (optional): Type of diff (`added`, `removed`, `added_revisions`, `removed_revisions`)

**Example:**
```json
{
  "snapshot1": "/home/user/snapshots/before-upgrade.snapshot",
  "snapshot2": "/home/user/snapshots/after-upgrade.snapshot",
  "diff_type": "added"
}
```

### 5. `analyze_namespaces`
Analyze namespace usage patterns including storage consumption and object counts.

**Parameters:**
- `snapshot` (required): Absolute path to the snapshot file to analyze
- `limit` (optional): Number of top namespaces to return (default: 10)

**Example:**
```json
{
  "snapshot": "/home/user/snapshots/cluster.snapshot",
  "limit": "5"
}
```

### 6. `get_snapshot_metadata`
Get comprehensive metadata about an etcd snapshot including storage statistics, fragmentation metrics, compaction info, quota usage, and key distribution.

**Parameters:**
- `snapshot` (required): Absolute path to the snapshot file to analyze

**Example:**
```json
{
  "snapshot": "/home/user/snapshots/cluster.snapshot"
}
```

**Returns:**
- Storage summary (total size, used size, free space, usage percentage)
- Fragmentation analysis (fragmentation ratio, bytes)
- Quota information (usage percentage, remaining space)
- Key distribution (total keys, revisions, unique keys)
- Value size statistics (average, largest, smallest)
- Compaction metrics (keys with multiple revisions, estimated savings)

### 7. `analyze_storage_health`
Perform comprehensive storage health analysis using snapshot metadata including fragmentation analysis, quota usage assessment, compaction efficiency, and optimization recommendations.

**Parameters:**
- `snapshot` (required): Absolute path to the snapshot file to analyze

**Example:**
```json
{
  "snapshot": "/home/user/snapshots/cluster.snapshot"
}
```

**Returns:**
- Storage efficiency analysis
- Fragmentation assessment
- Quota health status
- Revision pattern analysis
- Value size distribution
- Lease usage patterns
- Actionable recommendations for optimization

## Installation & Setup

### Prerequisites
- Go 1.24 or later
- octosql binary installed and in PATH
- etcd snapshots available on the filesystem

### Building the MCP Server

```bash
make build-mcp
```

### Running the MCP Server

```bash
# Run the server
./etcdsnapshot-mcp-server
```

### Configuration

The MCP server requires **absolute paths** for all snapshot parameters. This simplifies configuration and makes the server more flexible:

- **No environment variables needed**: The server no longer depends on `ETCD_SNAPSHOT_DIR`
- **Absolute paths required**: All snapshot parameters must be absolute paths (e.g., `/path/to/snapshot.db`)
- **Flexible snapshot locations**: Snapshots can be stored anywhere on the filesystem

## Usage Examples

### Integration with AI Tools

The MCP server can be integrated with various AI assistants and tools:

1. **Claude Desktop**: Use the MCP server to analyze cluster state
2. **Custom AI Applications**: Integrate via the MCP protocol
3. **Automated Monitoring**: Use for continuous cluster analysis


#### Integration with Cursor

You can load this project into cursor and install the mcp server:

```bash
make install
```

The local configuration in the .cursor folder uses the local build.
The MCP server should be automatically recognized and you can see the enabled tools in the chat window.

You can then place a snapshot (mind the '.snapshot' extension) anywhere on your filesystem and reference it by absolute path:

```  
$ ls -l /home/user/snapshots/
total 1742468
-rw-r--r--. 1 user user 1784283168 Jul 14 13:53 snapshot_2025_05_02_150554.snapshot
```  

A simple prompt like this:
> I want to run a performance analysis of my snapshot at /home/user/snapshots/snapshot_2025_05_02_150554.snapshot

should be able to return a meaningful response to you like:

``` 
Ran tool

Excellent! The performance analysis worked successfully! 🎉

## Performance Analysis Results

Here's what the analysis revealed about your etcd snapshot:

### Key Findings:

**📊 Cluster Scale:**
- **Max revision**: 1.16 billion revisions - this is a very active cluster

**🔥 Performance Hotspots:**
The biggest performance concerns are ArgoCD resources:

1. **`/kubernetes.io/argoproj.io/applicationsets/openshift-gitops/acm-policies`**
   - 1,201 revisions consuming 323 MB total storage
   - This is your biggest performance hotspot

2. **`/kubernetes.io/argoproj.io/applicationsets/openshift-gitops/cluster-configs-applicationset`**
   - 842 revisions consuming 227 MB total storage

3. **`/kubernetes.io/argoproj.io/applications/openshift-gitops/cluster-config-app-of-apps`**
   - 359 revisions consuming 99 MB total storage

**📈 High-Churn Resources:**
- ArgoCD ApplicationSets and Applications are being frequently modified
- API request counters are also showing high revision counts (though smaller impact)

**💾 Large Individual Values:**
- ArgoCD CRD definition: 848 KB
- Various ConfigMaps and Secrets: 400-600 KB each

### Recommendations:

1. **Investigate ArgoCD Configuration**: The ArgoCD ApplicationSets are causing excessive churn - consider reviewing sync policies and update patterns
2. **Monitor Write Patterns**: Multiple keys have frequent revisions suggesting very active GitOps workflows
3. **Consider Compaction**: With 1.2 billion revisions, regular etcd compaction would help performance
4. **Review ApplicationSet Logic**: The `acm-policies` ApplicationSet alone has 1,201 revisions - this suggests it's being updated very frequently

The analysis shows this is a very active OpenShift/Kubernetes cluster with heavy GitOps usage via ArgoCD, which is causing significant etcd churn.
```

You can also prompt for a more general analysis:

> I have an etcd snapshot in the home snapshots folder "a.snapshot" that I would like to analyse

```
I'll help you analyze your etcd snapshot file. Let me start with a comprehensive cluster analysis to give you an overview of the cluster health and resources.
Ran tool

Now let me get more detailed information about the resource distribution and performance characteristics of your cluster:
Ran tool
Ran tool

Let me also analyze namespace usage patterns to give you a complete picture:
Ran tool

Based on my analysis of your etcd snapshot, here's a comprehensive overview of your cluster:

## **Cluster Overview**
- **Total Resources**: 12,374 objects
- **Cluster Type**: OpenShift cluster
- **Max Revision**: 29,465 (indicates active cluster)

## **Top Namespaces by Storage Usage**

| Namespace | Size | Objects | Purpose |
|-----------|------|---------|---------|
| **customresourcedefinitions** | 4.46 MB | 145 | CRD definitions |
| **openshift-monitoring** | 2.17 MB | 657 | Monitoring stack |
| **openshift-kube-apiserver** | 1.73 MB | 546 | API server components |
| **openshift-config-managed** | 1.45 MB | 76 | Managed configurations |
| **openshift-etcd** | 1.17 MB | 449 | etcd-related resources |

## **Resource Distribution**

The cluster shows typical OpenShift patterns:
- **Events**: 6,204 (50% of all resources) - High activity cluster
- **Leases**: 1,327 (11%) - Leader election and coordination
- **Secrets**: 651 (5%) - Credentials and configs
- **ConfigMaps**: 548 (4%) - Configuration data
- **Pods**: 338 (3%) - Running workloads

## **Performance Insights**

**High-churn Resources** (frequent updates):
- `kube-controller-manager` lease: 145 revisions
- Master node leases: 40+ revisions each
- API server leases: 40+ revisions each

**Largest Objects**:
- `alertmanagerconfigs.monitoring.coreos.com` CRD: 544 KB
- Authentication branding template: 460 KB
- Prometheus CRD: 452 KB
- Monitoring rule files: 245 KB

## **Key Findings**

✅ **Healthy OpenShift Cluster**: Standard namespaces and resource patterns
✅ **Active Monitoring**: Comprehensive observability setup
✅ **High Activity**: 6,204 events indicate active workloads
⚠️ **High Lease Churn**: Heavy controller manager and node lease activity
📊 **Large CRDs**: Custom resource definitions consuming significant storage

This appears to be a production OpenShift cluster with active workloads and comprehensive monitoring. The high number of events and lease activity suggests normal operational activity.

Would you like me to analyze any specific aspects further, such as particular namespaces, resource types, or performance patterns?
```
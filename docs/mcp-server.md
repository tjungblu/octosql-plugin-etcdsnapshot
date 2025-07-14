# MCP Server for etcd Snapshot Analysis

This MCP (Model Context Protocol) server allows AI assistants to analyze Kubernetes/OpenShift cluster state by querying etcd snapshots using natural language.

## Overview

The MCP server exposes powerful cluster analysis capabilities through a set of tools that AI assistants can use to:

- Execute custom SQL queries against etcd snapshots
- Perform high-level cluster analysis
- Find specific Kubernetes resources
- Compare snapshots to detect changes

## Available Tools

### 1. `query_etcd`
Execute custom SQL queries against etcd snapshots.

**Parameters:**
- `query` (required): SQL query to execute
- `snapshot` (optional): Snapshot file to query (defaults to latest)

**Example:**
```json
{
  "query": "SELECT namespace, COUNT(*) as count FROM etcd.snapshot WHERE resourceType = 'pods' GROUP BY namespace ORDER BY count DESC LIMIT 10",
  "snapshot": "cluster-snapshot-2024-01-15.snapshot"
}
```

### 2. `analyze_cluster`
Perform high-level cluster analysis.

**Parameters:**
- `analysis_type` (required): Type of analysis (`overview`, `resources`, `performance`)
- `snapshot` (optional): Snapshot file to analyze

**Example:**
```json
{
  "analysis_type": "performance",
  "snapshot": "prod-cluster.snapshot"
}
```

### 3. `find_resources`
Find specific Kubernetes resources.

**Parameters:**
- `resource_type` (required): Type of resource (e.g., `pods`, `services`, `deployments`)
- `namespace` (optional): Namespace to search in
- `name` (optional): Resource name to search for
- `snapshot` (optional): Snapshot file to search in

**Example:**
```json
{
  "resource_type": "deployments",
  "namespace": "kube-system",
  "name": "coredns"
}
```

### 4. `compare_snapshots`
Compare two etcd snapshots to find differences.

**Parameters:**
- `snapshot1` (required): First snapshot file
- `snapshot2` (required): Second snapshot file
- `diff_type` (optional): Type of diff (`added`, `removed`, `modified`, `all`)

**Example:**
```json
{
  "snapshot1": "before-upgrade.snapshot",
  "snapshot2": "after-upgrade.snapshot",
  "diff_type": "modified"
}
```

## Installation & Setup

### Prerequisites
- Go 1.24 or later
- octosql binary installed and in PATH
- etcd snapshots available

### Building the MCP Server

```bash
make build-mcp
```

### Running the MCP Server

```bash
# Set snapshot directory (optional)
export ETCD_SNAPSHOT_DIR=/path/to/snapshots

# Run the server
./etcdsnapshot-mcp-server
```

### Configuration

The MCP server can be configured using environment variables:

- `ETCD_SNAPSHOT_DIR`: Directory containing etcd snapshots (default: `${HOME}/snapshots`)

## Usage Examples

### Integration with AI Tools

The MCP server can be integrated with various AI assistants and tools:

1. **Claude Desktop**: Use the MCP server to analyze cluster state
2. **Custom AI Applications**: Integrate via the MCP protocol
3. **Automated Monitoring**: Use for continuous cluster analysis


#### Integration with Cursor

You can load this project into cursor and build the mcp server as documented above:

```bash
make build-mcp
```

The local configuration in the .cursor folder uses the local build and snapshots folder.
The MCP server should be automatically recognized and you can see the enabled tools in the chat window.

You can then just simply plant a snapshot (mind the '.snapshot' extension) in the snapshot folder of your home directory:

```  
$ ls -l ${HOME}/snapshots
total 1742468
-rw-r--r--. 1 tjungblu tjungblu 1784283168 Jul 14 13:53 snapshot_2025_05_02_150554.snapshot
```  

A simple prompt like this:
> I want to run a performance analysis of my snapshot in the snapshots folder

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

If there are multiple snapshots, it will always take the newest (by modtime).



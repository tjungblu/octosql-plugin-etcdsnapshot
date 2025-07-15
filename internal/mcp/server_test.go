package mcp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewServer(t *testing.T) {
	config := Config{
		Name:        "test-server",
		Version:     "1.0.0",
		Description: "Test server description",
		SnapshotDir: t.TempDir(),
	}

	server, err := NewServer(config)
	require.NoError(t, err)
	require.NotNil(t, server)
	require.Equal(t, config, server.config)
	require.NotNil(t, server.queryEngine)
	require.NotNil(t, server.mcpServer)
}

func TestNewServerWithInvalidSnapshotDir(t *testing.T) {
	config := Config{
		Name:        "test-server",
		Version:     "1.0.0",
		Description: "Test server description",
		SnapshotDir: "/invalid/path/that/cannot/be/created",
	}

	_, err := NewServer(config)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to create query engine")
}

func TestServerConfig(t *testing.T) {
	config := Config{
		Name:        "etcd-snapshot-analyzer",
		Version:     "2.0.0",
		Description: "ETCD Snapshot Analysis Tool",
		SnapshotDir: t.TempDir(),
	}

	server, err := NewServer(config)
	require.NoError(t, err)
	require.Equal(t, "etcd-snapshot-analyzer", server.config.Name)
	require.Equal(t, "2.0.0", server.config.Version)
	require.Equal(t, "ETCD Snapshot Analysis Tool", server.config.Description)
}

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
	}

	server, err := NewServer(config)
	require.NoError(t, err)
	require.NotNil(t, server)
	require.Equal(t, config, server.config)
	require.NotNil(t, server.queryEngine)
	require.NotNil(t, server.mcpServer)
}

func TestNewServerAlwaysSucceeds(t *testing.T) {
	config := Config{
		Name:        "test-server",
		Version:     "1.0.0",
		Description: "Test server description",
	}

	server, err := NewServer(config)
	require.NoError(t, err)
	require.NotNil(t, server)
}

func TestServerConfig(t *testing.T) {
	config := Config{
		Name:        "etcd-snapshot-analyzer",
		Version:     "2.0.0",
		Description: "ETCD Snapshot Analysis Tool",
	}

	server, err := NewServer(config)
	require.NoError(t, err)
	require.Equal(t, "etcd-snapshot-analyzer", server.config.Name)
	require.Equal(t, "2.0.0", server.config.Version)
	require.Equal(t, "ETCD Snapshot Analysis Tool", server.config.Description)
}

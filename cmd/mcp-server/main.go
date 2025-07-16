package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tjungblu/octosql-plugin-etcdsnapshot/internal/mcp"
)

func main() {
	// Create a context that can be cancelled on signal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Initialize the MCP server with etcd snapshot capabilities
	server, err := mcp.NewServer(mcp.Config{
		Name:        "etcd-snapshot-analyzer",
		Version:     "1.0.0",
		Description: "MCP server for analyzing etcd snapshots from Kubernetes/OpenShift clusters",
	})
	if err != nil {
		log.Fatalf("Failed to create MCP server: %v", err)
	}

	// Start the server in a goroutine
	go func() {
		if err := server.Start(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				// Normal termination when stdin is closed - trigger shutdown
				cancel()
			} else {
				log.Fatalf("Failed to start MCP server: %v", err)
			}
		}
	}()

	// Wait for shutdown signal or context cancellation
	select {
	case <-sigChan:
		log.Println("Shutting down MCP server...")
	case <-ctx.Done():
		// Context was cancelled (e.g., due to EOF)
		log.Println("MCP server terminated normally")
	}
	cancel()
}

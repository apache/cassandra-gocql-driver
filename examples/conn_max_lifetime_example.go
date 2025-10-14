// Example demonstrating the ConnMaxLifetime feature
// This example shows how to configure connection maximum lifetime
// to automatically close and replace connections after a specified duration.

package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gocql/gocql"
)

func main() {
	// Create a cluster configuration
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "system"
	cluster.Consistency = gocql.Quorum
	
	// Set the maximum lifetime for connections
	// Connections older than this will be closed and replaced
	// This is useful for:
	// 1. Load balancing across backend servers
	// 2. Preventing stale connections
	// 3. Ensuring connections don't accumulate issues over time
	cluster.ConnMaxLifetime = 5 * time.Minute
	
	// Create session
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal("Failed to create session:", err)
	}
	defer session.Close()
	
	fmt.Println("Session created successfully!")
	fmt.Printf("ConnMaxLifetime is set to: %v\n", cluster.ConnMaxLifetime)
	fmt.Println("\nConnections will be automatically closed and replaced after 5 minutes.")
	fmt.Println("This happens transparently during connection pool operations.")
	
	// Example query
	var clusterName string
	if err := session.Query("SELECT cluster_name FROM system.local").Scan(&clusterName); err != nil {
		log.Fatal("Query failed:", err)
	}
	
	fmt.Printf("\nConnected to cluster: %s\n", clusterName)
}


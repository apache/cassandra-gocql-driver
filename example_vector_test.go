/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql_test

import (
	"context"
	"fmt"
	"log"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// Example_vector demonstrates how to work with vector search in Cassandra 5.0+.
// This example shows Cassandra's native vector search capabilities using ANN (Approximate Nearest Neighbor)
// search with ORDER BY ... ANN OF syntax for finding similar vectors.
// Note: Requires Cassandra 5.0+ and a SAI index on the vector column for ANN search.
func Example_vector() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.vectors(
		id int,
		item_name text,
		embedding vector<float, 5>,
		PRIMARY KEY(id)
	);
	-- Create SAI index for vector search (required for ANN)
	CREATE INDEX IF NOT EXISTS ann_index ON example.vectors(embedding) USING 'sai';
	*/
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	ctx := context.Background()

	// Create the table first (if it doesn't exist)
	err = session.Query(`CREATE TABLE IF NOT EXISTS example.vectors(
		id int, 
		item_name text, 
		embedding vector<float, 5>, 
		PRIMARY KEY(id)
	)`).ExecContext(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Create SAI index for vector search (required for ANN search)
	fmt.Println("Creating SAI index for vector search...")
	err = session.Query(`CREATE INDEX IF NOT EXISTS ann_index 
		ON example.vectors(embedding) USING 'sai'`).ExecContext(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Insert sample vectors representing different items
	// These could be embeddings from ML models for products, documents, etc.
	vectorData := []struct {
		id     int
		name   string
		vector []float32
	}{
		{1, "apple", []float32{0.8, 0.2, 0.1, 0.9, 0.3}},
		{2, "orange", []float32{0.7, 0.3, 0.2, 0.8, 0.4}},
		{3, "banana", []float32{0.6, 0.4, 0.9, 0.2, 0.7}},
		{4, "grape", []float32{0.9, 0.1, 0.3, 0.7, 0.5}},
		{5, "watermelon", []float32{0.2, 0.8, 0.6, 0.4, 0.9}},
		{6, "strawberry", []float32{0.8, 0.3, 0.2, 0.9, 0.4}},
		{7, "pineapple", []float32{0.3, 0.7, 0.8, 0.1, 0.6}},
		{8, "mango", []float32{0.7, 0.4, 0.5, 0.8, 0.2}},
	}

	// Insert all vectors
	fmt.Println("Inserting sample vectors...")
	for _, item := range vectorData {
		err = session.Query("INSERT INTO example.vectors (id, item_name, embedding) VALUES (?, ?, ?)",
			item.id, item.name, item.vector).ExecContext(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Define a query vector (e.g., searching for items similar to "apple-like" characteristics)
	queryVector := []float32{0.8, 0.2, 0.1, 0.9, 0.3}
	fmt.Printf("Searching for vectors similar to: %v\n\n", queryVector)

	// Perform ANN (Approximate Nearest Neighbor) search using ORDER BY ... ANN OF
	// This finds the 3 most similar vectors to our query vector
	fmt.Println("Top 3 most similar items (using ANN search):")
	iter := session.Query(`
		SELECT id, item_name, embedding
		FROM example.vectors 
		ORDER BY embedding ANN OF ? 
		LIMIT 3`,
		queryVector,
	).IterContext(ctx)

	for {
		var id int
		var itemName string
		var embedding []float32

		if !iter.Scan(&id, &itemName, &embedding) {
			break
		}
		fmt.Printf("  %s (ID: %d) - Vector: %v\n", itemName, id, embedding)
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

	fmt.Println()

	// Perform similarity search with a different query vector
	queryVector2 := []float32{0.2, 0.8, 0.6, 0.4, 0.9}
	fmt.Printf("Searching for vectors similar to: %v\n", queryVector2)
	fmt.Println("Top 4 most similar items:")

	scanner := session.Query(`
		SELECT id, item_name, embedding
		FROM example.vectors 
		ORDER BY embedding ANN OF ? 
		LIMIT 4`,
		queryVector2,
	).IterContext(ctx).Scanner()

	for scanner.Next() {
		var id int
		var itemName string
		var embedding []float32

		err = scanner.Scan(&id, &itemName, &embedding)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("  %s (ID: %d) - Vector: %v\n", itemName, id, embedding)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Println()

	// Basic vector retrieval (traditional approach)
	fmt.Println("Basic vector retrieval by ID:")
	var id int
	var itemName string
	var embedding []float32
	iter = session.Query("SELECT id, item_name, embedding FROM example.vectors WHERE id = ?", 1).
		IterContext(ctx)
	if !iter.Scan(&id, &itemName, &embedding) {
		log.Fatal(iter.Close())
	}
	fmt.Printf("  %s (ID: %d) - Vector: %v\n", itemName, id, embedding)

	fmt.Println()

	// Show all vectors for comparison
	fmt.Println("All vectors in the database:")
	allVectors := session.Query("SELECT id, item_name, embedding FROM example.vectors").IterContext(ctx)
	for {
		var id int
		var itemName string
		var embedding []float32

		if !allVectors.Scan(&id, &itemName, &embedding) {
			break
		}
		fmt.Printf("  %s (ID: %d) - Vector: %v\n", itemName, id, embedding)
	}
	if err := allVectors.Close(); err != nil {
		log.Fatal(err)
	}

	// Example output:
	// Creating SAI index for vector search...
	// Inserting sample vectors...
	// Searching for vectors similar to: [0.8 0.2 0.1 0.9 0.3]
	//
	// Top 3 most similar items (using ANN search):
	//   apple (ID: 1) - Vector: [0.8 0.2 0.1 0.9 0.3]
	//   strawberry (ID: 6) - Vector: [0.8 0.3 0.2 0.9 0.4]
	//   orange (ID: 2) - Vector: [0.7 0.3 0.2 0.8 0.4]
}

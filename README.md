# MCP Cassandra

A Model Context Protocol (MCP) server for interacting with Apache Cassandra databases.

## Overview

MCP Cassandra provides a set of tools that allow AI assistants to interact with Cassandra databases through the Model Context Protocol. This enables AI models to query, modify, and manage Cassandra databases directly.

## Features

- Connect to and disconnect from Cassandra clusters
- List keyspaces and tables
- Describe table schemas and view CREATE TABLE statements
- Execute CQL queries
- Select data from tables with filtering capabilities
- List secondary indexes and materialized views
- Support for authentication and secure connections
- Robust error handling and recovery mechanisms
- Support for complex Cassandra data types including maps, lists, and sets

## Installation

```bash
npm install -g mcp-cassandra
```

## Usage

### Starting the server

```bash
mcp-cassandra
```

The server will start and register itself with the MCP registry.

## Available Tools

### `cassandra_connect`
Connects to a Cassandra cluster. The connection configuration is saved for all subsequent operations.

**Parameters:**
```javascript
{
  "contactPoints": ["localhost"],      // Required: Array of Cassandra nodes
  "localDataCenter": "datacenter1",    // Required: Your local data center name
  "keyspace": "my_keyspace",           // Optional: Default keyspace to use
  "username": "cassandra",             // Optional: Authentication username
  "password": "cassandra",             // Optional: Authentication password
  "port": 9042,                        // Optional: Connection port (default: 9042)
  "queryOptions": {                    // Optional: Default options for queries
    "consistency": "LOCAL_ONE",        // Optional: Default consistency level
    "fetchSize": 1000,                 // Optional: Default fetch size (for pagination)
    "prepare": true                    // Optional: Whether to prepare queries by default
  },
  "socketOptions": {                   // Optional: Socket configuration
    "connectTimeout": 5000,            // Optional: Connection timeout in ms
    "readTimeout": 12000,              // Optional: Read timeout in ms
    "keepAlive": true                  // Optional: Keep connection alive
  }
}
```

### `cassandra_disconnect`
Disconnects from the current Cassandra connection.

**Parameters:** None required

### `cassandra_list_keyspaces`
Lists all keyspaces in the connected cluster.

**Parameters:**
```javascript
{
  "includeSystemKeyspaces": false     // Optional: Include system keyspaces (default: false)
}
```

### `cassandra_list_tables`
Lists all tables in a keyspace with detailed information.

**Parameters:**
```javascript
{
  "keyspaceName": "metrics_keyspace",  // Required: Keyspace name to list tables from
  "includeViews": true                 // Optional: Include materialized views (default: false)
}
```

**Response:**
```javascript
{
  "success": true,
  "keyspace": "metrics_keyspace",
  "tables": ["table1", "table2", "view1"],
  "count": {
    "total": 3,
    "tables": 2,
    "views": 1
  }
}
```

### `cassandra_describe_table`
Gets detailed schema information about a table.

**Parameters:**
```javascript
{
  "keyspaceName": "metrics_keyspace",  // Required: Keyspace name
  "tableName": "dcgm_data"             // Required: Table name
}
```

**Response:**
```javascript
{
  "name": "dcgm_data",
  "keyspace": "metrics_keyspace",
  "columns": [
    {
      "name": "id",
      "type": "uuid",
      "kind": "partition_key"
    },
    {
      "name": "timestamp",
      "type": "timestamp",
      "kind": "clustering_key"
    },
    {
      "name": "metric_value",
      "type": "double",
      "kind": "regular"
    },
    {
      "name": "labels",
      "type": "map<text, text>",
      "kind": "regular"
    }
  ],
  "partitionKeys": ["id"],
  "clusteringKeys": ["timestamp"],
  "clusteringOrder": [
    {
      "column": "timestamp",
      "order": "ASC"
    }
  ],
  "tableOptions": {
    // Various table options...
  }
}
```

### `cassandra_execute_query`
Executes a CQL query with support for prepared statements.

**Parameters:**
```javascript
{
  "query": "SELECT * FROM users WHERE id = ?",  // Required: CQL query string
  "params": ["user123"],                        // Optional: Parameters for prepared statement
  "consistency": "LOCAL_QUORUM",                // Optional: Consistency level for this query
  "fetchSize": 100,                             // Optional: Fetch size for pagination
  "pageState": "ABC123..."                      // Optional: Page state token for pagination
}
```

### `cassandra_select_rows`
Selects rows from a table with enhanced filtering and pagination.

**Parameters:**
```javascript
{
  "keyspaceName": "metrics_keyspace",           // Required: Keyspace name
  "tableName": "dcgm_data",                     // Required: Table name
  "selectColumns": ["id", "timestamp", "value"], // Optional: Columns to select (default: all)
  "whereClauses": {                              // Optional: WHERE conditions as key-value pairs
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "status": "active"
  },
  "orderBy": {                                   // Optional: ORDER BY clause
    "column": "timestamp",
    "order": "DESC"
  },
  "limit": 50,                                   // Optional: LIMIT clause value
  "allowFiltering": true,                        // Optional: Add ALLOW FILTERING clause
  "consistency": "LOCAL_QUORUM",                 // Optional: Consistency level
  "fetchSize": 50,                               // Optional: Pagination fetch size
  "pageState": "ABC123..."                       // Optional: Page state token
}
```

**Response:**
```javascript
{
  "rows": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "timestamp": "2023-09-15T12:00:00.000Z",
      "value": 42.5,
      "labels": {                                // Map type is properly converted to object
        "host": "server01",
        "region": "us-west-2"
      }
    },
    // More rows...
  ],
  "columns": [
    { "name": "id", "type": "uuid" },
    { "name": "timestamp", "type": "timestamp" },
    { "name": "value", "type": "double" },
    { "name": "labels", "type": "map<text, text>" }
  ],
  "pageState": "DEF456...",                     // Present if there are more results
  "warnings": [],
  "info": {
    "queriedHost": "127.0.0.1:9042",
    "triedHosts": ["127.0.0.1:9042"],
    "achievedConsistency": "LOCAL_ONE"
  }
}
```

## Advanced Usage and Troubleshooting

### Handling Complex Data Types

MCP Cassandra properly handles all Cassandra data types including:

- **Maps**: Converted to JavaScript objects
- **Lists**: Maintained as arrays
- **Sets**: Converted to arrays
- **Tuples**: Represented as arrays
- **UDTs**: Converted to nested objects
- **Blobs**: Converted to hex strings for JSON compatibility
- **Dates/Timestamps**: Converted to ISO strings

### Common Error Messages and Solutions

#### Connection Issues

- **"Failed to connect: Connection refused"**: Ensure Cassandra is running at the specified host:port and network connectivity is available.
- **"Authentication failed"**: Check username and password credentials.
- **"No contact points available"**: Verify contact points are correctly specified.

#### Query Issues

- **"Unconfigured table"**: The table doesn't exist. Check the keyspace and table names.
- **"Undefined column name"**: Column name is misspelled or doesn't exist.
- **"ALLOW FILTERING required"**: Add `allowFiltering: true` to your query (use cautiously as it may cause performance issues).
- **"Invalid order by clause"**: In Cassandra, you can only order by clustering columns.

#### Data Type Issues

- **"Type error: map field expected"**: Check that you're using compatible data types for map fields.
- **"Invalid value for column"**: Value doesn't match the column type. Check data formats.

## Development

### Prerequisites

- Node.js (v18.19.0 or higher)
- Cassandra database (for testing)

### Setup

1. Clone the repository
2. Install dependencies:
   ```bash
   npm install
   ```
3. Build the project:
   ```bash
   npm run build
   ```

### Development workflow

- Use `npm run watch` to continuously compile TypeScript files during development
- Use `npm start` to run the server locally
- Use `npm test` to run the test suite

## License

MIT 
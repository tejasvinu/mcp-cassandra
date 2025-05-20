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

### Available Tools

- `cassandra_connect`: Connect to a Cassandra cluster
- `cassandra_disconnect`: Disconnect from the current Cassandra connection
- `cassandra_list_keyspaces`: List all keyspaces in the connected cluster, with option to include system keyspaces
- `cassandra_list_tables`: List all tables in a keyspace with option to include materialized views
- `cassandra_describe_table`: Get detailed schema information about a table including columns, data types, primary keys, and options
- `cassandra_show_create_table`: Generate the CQL CREATE TABLE statement for a table
- `cassandra_list_indexes`: List all secondary indexes and materialized views in a keyspace or for a specific table
- `cassandra_execute_query`: Execute a CQL query with support for prepared statements
- `cassandra_select_rows`: Select rows from a table with filtering and pagination

## Tool Examples

### Connect to a Cassandra cluster

```javascript
{
  "contactPoints": ["localhost"],
  "localDataCenter": "datacenter1",
  "keyspace": "my_keyspace",  // optional
  "username": "cassandra",    // optional
  "password": "cassandra"     // optional
}
```

### List keyspaces including system keyspaces

```javascript
{
  "includeSystemKeyspaces": true
}
```

### List tables and materialized views in a keyspace

```javascript
{
  "keyspaceName": "metrics_keyspace",
  "includeViews": true
}
```

### Describe a table

```javascript
{
  "keyspaceName": "metrics_keyspace",
  "tableName": "dcgm_data"
}
```

### Show CREATE TABLE statement

```javascript
{
  "keyspaceName": "metrics_keyspace",
  "tableName": "dcgm_data"
}
```

### Execute a CQL query

```javascript
{
  "query": "SELECT * FROM users WHERE id = ?",
  "params": ["user123"],
  "consistency": "LOCAL_QUORUM"
}
```

### List indexes for a table

```javascript
{
  "keyspaceName": "metrics_keyspace",
  "tableName": "dcgm_data"
}
```

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

## License

MIT 
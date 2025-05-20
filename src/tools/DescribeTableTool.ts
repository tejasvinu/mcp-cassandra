import { MCPTool } from "mcp-framework";
import { z } from "zod";
import { getCassandraClient } from "../cassandra-client.js";
// Assuming 'metadata' is imported but its members are problematic
// import { metadata } from 'cassandra-driver'; 

interface DescribeTableInput {
  keyspaceName: string;
  tableName: string;
}

class DescribeTableTool extends MCPTool<DescribeTableInput> {
  name = "cassandra_describe_table";
  description = "Describes a table in a specified Cassandra keyspace, showing its columns, data types, primary key, and table options.";

  schema = {
    keyspaceName: {
        type: z.string().nonempty(),
        description: "The name of the keyspace containing the table."
    },
    tableName: {
      type: z.string().nonempty(),
      description: "The name of the table to describe."
    }
  };

  // Safely converts column type to string representation
  private getColumnTypeString(type: any): string {
    if (!type) {
      return 'unknown';
    }
    
    try {
      // First try to use the inspect method if available
      if (typeof type.inspect === 'function') {
        return type.inspect();
      }
      
      // Fall back to name property
      if (type.name) {
        return type.name;
      }
      
      // Try to get the code property
      if (type.code !== undefined) {
        return `type_${type.code}`;
      }
      
      // Last resort, convert the whole object to string
      return type.toString();
    } catch (error) {
      console.error('Error converting column type to string:', error);
      // If all else fails, use the constructor name or 'unknown'
      return type.constructor ? type.constructor.name : 'unknown';
    }
  }

  async execute(input: DescribeTableInput) {
    const client = getCassandraClient();
    try {
      // Use the raw query method to get comprehensive table information
      const query = `SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?`;
      const tableResult = await client.execute(query, [input.keyspaceName, input.tableName], { prepare: true });
      
      if (tableResult.rows.length === 0) {
        return `Table '${input.keyspaceName}.${input.tableName}' not found.`;
      }
      
      // Get column information
      const columnsQuery = `SELECT * FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?`;
      const columnsResult = await client.execute(columnsQuery, [input.keyspaceName, input.tableName], { prepare: true });
      
      // Get primary key information
      const keysQuery = `SELECT * FROM system_schema.key_columns WHERE keyspace_name = ? AND table_name = ?`;
      const keysResult = await client.execute(keysQuery, [input.keyspaceName, input.tableName], { prepare: true });
      
      // Sort key columns by position
      const keyColumns = keysResult.rows.sort((a, b) => a.position - b.position);
      
      // Separate partition keys and clustering keys
      const partitionKeys = keyColumns.filter(key => key.kind === 'partition_key').map(key => key.column_name);
      const clusteringKeys = keyColumns.filter(key => key.kind === 'clustering').map(key => key.column_name);
      
      // Get table options from tableResult
      const tableOptions = tableResult.rows[0];
      
      // Format columns with type information
      const columns = columnsResult.rows.map(col => ({
        name: col.column_name,
        type: col.type,
        position: col.position,
        kind: partitionKeys.includes(col.column_name) ? 'partition_key' : 
              clusteringKeys.includes(col.column_name) ? 'clustering_key' : 'regular'
      }));
      
      // Get clustering order
      const clusteringOrder = columns
        .filter(col => col.kind === 'clustering_key')
        .map(col => {
          const keyInfo = keyColumns.find(k => k.column_name === col.name);
          return {
            column: col.name,
            order: keyInfo && keyInfo.clustering_order ? keyInfo.clustering_order.toUpperCase() : 'ASC'
          };
        });
      
      return {
        name: input.tableName,
        keyspace: input.keyspaceName,
        columns,
        partitionKeys,
        clusteringKeys,
        clusteringOrder,
        tableOptions: {
          bloomFilterFpChance: tableOptions.bloom_filter_fp_chance,
          caching: tableOptions.caching,
          comment: tableOptions.comment,
          compaction: tableOptions.compaction,
          compression: tableOptions.compression,
          crcCheckChance: tableOptions.crc_check_chance,
          dcLocalReadRepairChance: tableOptions.dclocal_read_repair_chance,
          defaultTimeToLive: tableOptions.default_time_to_live,
          gcGraceSeconds: tableOptions.gc_grace_seconds,
          maxIndexInterval: tableOptions.max_index_interval,
          memtableFlushPeriodInMs: tableOptions.memtable_flush_period_in_ms,
          minIndexInterval: tableOptions.min_index_interval,
          readRepairChance: tableOptions.read_repair_chance,
          speculative_retry: tableOptions.speculative_retry
        }
      };
    } catch (error: any) {
      console.error(`Error describing table ${input.keyspaceName}.${input.tableName}:`, error);
      
      // Try an alternative approach if the first fails - directly query system tables
      try {
        // Check if table exists
        const tableCheckQuery = `SELECT table_name FROM system_schema.tables 
                                WHERE keyspace_name = ? AND table_name = ?`;
        const tableCheck = await client.execute(tableCheckQuery, [input.keyspaceName, input.tableName], { prepare: true });
        
        if (tableCheck.rows.length === 0) {
          return `Table '${input.keyspaceName}.${input.tableName}' not found.`;
        }
        
        // Get column information
        const columnsQuery = `SELECT column_name, type FROM system_schema.columns 
                             WHERE keyspace_name = ? AND table_name = ?`;
        const columnsResult = await client.execute(columnsQuery, [input.keyspaceName, input.tableName], { prepare: true });
        
        // Get primary key information
        const keysQuery = `SELECT column_name, kind, position FROM system_schema.key_columns 
                          WHERE keyspace_name = ? AND table_name = ?`;
        const keysResult = await client.execute(keysQuery, [input.keyspaceName, input.tableName], { prepare: true });
        
        // Separate partition keys and clustering keys
        const keyColumns = keysResult.rows;
        const partitionKeys = keyColumns
          .filter(key => key.kind === 'partition_key')
          .sort((a, b) => a.position - b.position)
          .map(key => key.column_name);
          
        const clusteringKeys = keyColumns
          .filter(key => key.kind === 'clustering')
          .sort((a, b) => a.position - b.position)
          .map(key => key.column_name);
        
        // Format columns with type information
        const columns = columnsResult.rows.map(col => ({
          name: col.column_name,
          type: col.type,
          kind: partitionKeys.includes(col.column_name) ? 'partition_key' : 
                clusteringKeys.includes(col.column_name) ? 'clustering_key' : 'regular'
        }));
        
        return {
          name: input.tableName,
          keyspace: input.keyspaceName,
          columns,
          partitionKeys,
          clusteringKeys
        };
      } catch (fallbackError: any) {
        console.error(`Fallback error describing table ${input.keyspaceName}.${input.tableName}:`, fallbackError);
        return `Failed to describe table '${input.keyspaceName}.${input.tableName}': ${error.message}`;
      }
    }
  }
}

export default DescribeTableTool; 
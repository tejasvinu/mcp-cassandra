import { MCPTool } from "mcp-framework";
import { z } from "zod";
import { getCassandraClient } from "../cassandra-client.js";

interface ShowTableSchemaInput {
  keyspaceName: string;
  tableName: string;
}

class ShowTableSchemaTool extends MCPTool<ShowTableSchemaInput> {
  name = "cassandra_show_create_table";
  description = "Shows the CQL 'CREATE TABLE' statement that can be used to recreate the specified table.";

  schema = {
    keyspaceName: {
      type: z.string().nonempty(),
      description: "The name of the keyspace containing the table."
    },
    tableName: {
      type: z.string().nonempty(),
      description: "The name of the table to show creation statement for."
    }
  };

  async execute(input: ShowTableSchemaInput) {
    const client = getCassandraClient();
    try {
      // Check if table exists
      const tableCheckQuery = `SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?`;
      const tableCheck = await client.execute(tableCheckQuery, [input.keyspaceName, input.tableName], { prepare: true });
      
      if (tableCheck.rows.length === 0) {
        return { message: `Table '${input.keyspaceName}.${input.tableName}' not found.` };
      }
      
      const tableInfo = tableCheck.rows[0];
      
      // Get column information
      const columnsQuery = `SELECT * FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ? ORDER BY position ASC`;
      const columnsResult = await client.execute(columnsQuery, [input.keyspaceName, input.tableName], { prepare: true });
      
      if (columnsResult.rows.length === 0) {
        return { message: `No columns found for table '${input.keyspaceName}.${input.tableName}'.` };
      }
      
      // Get primary key information
      const keysQuery = `SELECT * FROM system_schema.key_columns WHERE keyspace_name = ? AND table_name = ? ORDER BY position ASC`;
      const keysResult = await client.execute(keysQuery, [input.keyspaceName, input.tableName], { prepare: true });
      
      // Separate partition keys and clustering keys
      const partitionKeys = keysResult.rows
        .filter(key => key.kind === 'partition_key')
        .sort((a, b) => a.position - b.position)
        .map(key => key.column_name);
        
      const clusteringKeys = keysResult.rows
        .filter(key => key.kind === 'clustering')
        .sort((a, b) => a.position - b.position)
        .map(key => {
          return {
            name: key.column_name,
            order: key.clustering_order?.toUpperCase() || 'ASC'
          };
        });
      
      // Start building the CREATE TABLE statement
      let createStatement = `CREATE TABLE ${input.keyspaceName}.${input.tableName} (\n`;
      
      // Add columns
      columnsResult.rows.forEach((col, index) => {
        createStatement += `  ${col.column_name} ${col.type}`;
        if (index < columnsResult.rows.length - 1) {
          createStatement += ',\n';
        }
      });
      
      // Add primary key clause if exists
      if (partitionKeys.length > 0 || clusteringKeys.length > 0) {
        createStatement += ',\n  PRIMARY KEY (';
        
        // Handle partition keys
        if (partitionKeys.length === 1) {
          createStatement += partitionKeys[0];
        } else if (partitionKeys.length > 1) {
          createStatement += `(${partitionKeys.join(', ')})`;
        }
        
        // Add clustering keys if they exist
        if (clusteringKeys.length > 0) {
          createStatement += `, ${clusteringKeys.map(key => key.name).join(', ')}`;
        }
        
        createStatement += ')';
      }
      
      createStatement += '\n)';
      
      // Add clustering order
      if (clusteringKeys.length > 0) {
        const clusteringOrders = clusteringKeys
          .map(key => `${key.name} ${key.order}`)
          .join(', ');
          
        createStatement += ` WITH CLUSTERING ORDER BY (${clusteringOrders})`;
      }
      
      // Add table options
      const optionsToInclude = ['bloom_filter_fp_chance', 'caching', 'comment', 'compaction', 
                            'compression', 'crc_check_chance', 'default_time_to_live', 
                            'gc_grace_seconds', 'read_repair_chance', 'speculative_retry'];
                            
      const tableOptions = Object.entries(tableInfo)
        .filter(([key]) => optionsToInclude.includes(key) && tableInfo[key] !== null)
        .map(([key, value]) => {
          // Format value based on type
          let formattedValue = value;
          if (typeof value === 'object') {
            formattedValue = JSON.stringify(value).replace(/"/g, "'");
          } else if (typeof value === 'string') {
            formattedValue = `'${value}'`;
          }
          
          // Convert snake_case to camelCase for readability
          const camelCaseKey = key.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());
          
          return `  ${camelCaseKey} = ${formattedValue}`;
        });
        
      if (tableOptions.length > 0) {
        if (clusteringKeys.length === 0) {
          createStatement += ' WITH\n';
        } else {
          createStatement += ' AND\n';
        }
        createStatement += tableOptions.join(' AND\n');
      }
      
      createStatement += ';';
      
      return { schema: createStatement };
    } catch (error: any) {
      console.error(`Error getting schema for ${input.keyspaceName}.${input.tableName}:`, error);
      return { error: `Failed to get schema for table '${input.keyspaceName}.${input.tableName}': ${error.message}` };
    }
  }
}

export default ShowTableSchemaTool; 
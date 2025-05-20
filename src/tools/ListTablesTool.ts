import { MCPTool } from "mcp-framework";
import { z } from "zod";
import { getCassandraClient } from "../cassandra-client.js";
// import { metadata } from 'cassandra-driver'; // Commented out due to type resolution issues

interface ListTablesInput {
  keyspaceName: string;
  includeViews?: boolean;
}

class ListTablesTool extends MCPTool<ListTablesInput> {
  name = "cassandra_list_tables";
  description = "Lists all tables in a specified Cassandra keyspace. Can optionally include materialized views.";

  schema = {
    keyspaceName: {
      type: z.string().nonempty(),
      description: "The name of the keyspace for which to list tables."
    },
    includeViews: {
      type: z.boolean().optional().default(false),
      description: "Whether to include materialized views in the results (default: false)"
    }
  };

  async execute(input: ListTablesInput) {
    const client = getCassandraClient();
    try {
      // Query system_schema.tables directly for more reliable results
      const query = `SELECT table_name, flags FROM system_schema.tables WHERE keyspace_name = ?`;
      const result = await client.execute(query, [input.keyspaceName], { prepare: true });
      
      if (result.rows.length === 0) {
        // Check if the keyspace exists
        const keyspaceQuery = `SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?`;
        const keyspaceResult = await client.execute(keyspaceQuery, [input.keyspaceName], { prepare: true });
        
        if (keyspaceResult.rows.length === 0) {
          return `Keyspace '${input.keyspaceName}' not found.`;
        }
        return `No tables found in keyspace '${input.keyspaceName}'.`;
      }
      
      let tables = result.rows.map(row => ({
        name: row.table_name,
        isView: (row.flags || []).includes('materialized_view')
      }));
      
      // Filter out views if not requested
      if (!input.includeViews) {
        tables = tables.filter(table => !table.isView);
      }
      
      if (tables.length === 0) {
        return `No ${input.includeViews ? 'tables or views' : 'tables'} found in keyspace '${input.keyspaceName}'.`;
      }
      
      // Just return table names for simplicity
      return tables.map(table => table.name).sort();
    } catch (error: any) {
      console.error(`Error listing tables for keyspace ${input.keyspaceName}:`, error);
      
      // Try alternative method using CQL directly
      try {
        // Execute query directly against system_schema
        const fallbackQuery = `SELECT table_name FROM system_schema.tables 
                               WHERE keyspace_name = ?`;
        const fallbackResult = await client.execute(fallbackQuery, [input.keyspaceName], { prepare: true });
        
        if (fallbackResult.rows.length === 0) {
          // Check if keyspace exists
          const keyspaceCheck = await client.execute(
            `SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?`, 
            [input.keyspaceName], 
            { prepare: true }
          );
          
          if (keyspaceCheck.rows.length === 0) {
            return `Failed to list tables: Keyspace '${input.keyspaceName}' not found.`;
          }
          return `No tables found in keyspace '${input.keyspaceName}'.`;
        }
        
        const tableNames = fallbackResult.rows.map(row => row.table_name);
        return tableNames.sort();
      } catch (fallbackError: any) {
        console.error(`Fallback error listing tables for keyspace ${input.keyspaceName}:`, fallbackError);
        return `Failed to list tables in keyspace '${input.keyspaceName}': ${error.message}`;
      }
    }
  }
}

export default ListTablesTool; 
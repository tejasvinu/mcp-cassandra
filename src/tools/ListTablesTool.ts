import { MCPTool } from "mcp-framework";
import { z } from "zod";
import { getCassandraClient } from "../cassandra-client.js";
import { types } from 'cassandra-driver';

interface ListTablesInput {
  keyspaceName: string;
  includeViews?: boolean;
}

// Define interfaces for table objects
interface TableInfo {
  name: string;
  isView: boolean;
}

interface TableResult {
  success: boolean;
  error: string | null;
  tables: string[];
  viewCount?: number;
  tableCount?: number;
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

  // Helper to verify if a keyspace exists
  private async checkKeyspaceExists(client: any, keyspaceName: string): Promise<boolean> {
    try {
      const keyspaceQuery = `SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?`;
      const keyspaceResult = await client.execute(keyspaceQuery, [keyspaceName], { prepare: true });
      return keyspaceResult.rows.length > 0;
    } catch (error: any) {
      console.error(`Error checking if keyspace exists: ${keyspaceName}`, error);
      return false;
    }
  }

  // Get tables using system_schema directly
  private async getTables(client: any, keyspaceName: string, includeViews: boolean): Promise<TableResult> {
    // We'll try multiple strategies to get the tables
    try {
      // Strategy 1: Query system_schema.tables directly with flags
      const query = `SELECT table_name, flags FROM system_schema.tables WHERE keyspace_name = ?`;
      const result = await client.execute(query, [keyspaceName], { prepare: true });
      
      if (result.rows.length === 0) {
        return { success: false, error: null, tables: [] };
      }
      
      let tables: TableInfo[] = result.rows.map((row: any) => ({
        name: row.table_name,
        isView: (row.flags || []).includes('materialized_view')
      }));
      
      // Filter out views if not requested
      if (!includeViews) {
        tables = tables.filter((table: TableInfo) => !table.isView);
      }
      
      return { 
        success: true, 
        error: null, 
        tables: tables.map((t: TableInfo) => t.name).sort(),
        viewCount: tables.filter((t: TableInfo) => t.isView).length,
        tableCount: tables.filter((t: TableInfo) => !t.isView).length
      };
    } catch (error: any) {
      console.error(`Error in strategy 1 for list tables: ${keyspaceName}`, error);
      // Continue to fallback strategies
    }
    
    try {
      // Strategy 2: Simple query against system_schema.tables without flags
      const fallbackQuery = `SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?`;
      const fallbackResult = await client.execute(fallbackQuery, [keyspaceName], { prepare: true });
      
      // No error but no tables found
      if (fallbackResult.rows.length === 0) {
        return { success: false, error: null, tables: [] };
      }
      
      // Get views separately if needed
      let views: string[] = [];
      if (includeViews) {
        try {
          const viewsQuery = `SELECT view_name FROM system_schema.views WHERE keyspace_name = ?`;
          const viewsResult = await client.execute(viewsQuery, [keyspaceName], { prepare: true });
          views = viewsResult.rows.map((row: any) => row.view_name);
        } catch (viewError) {
          console.error(`Error getting views for keyspace ${keyspaceName}`, viewError);
          // Continue without views
        }
      }
      
      const tables = fallbackResult.rows.map((row: any) => row.table_name);
      
      return { 
        success: true, 
        error: null, 
        tables: [...tables, ...views].sort(),
        viewCount: views.length,
        tableCount: tables.length
      };
    } catch (error: any) {
      console.error(`Error in strategy 2 for list tables: ${keyspaceName}`, error);
      return { 
        success: false, 
        error: error.message, 
        tables: []
      };
    }
  }

  async execute(input: ListTablesInput) {
    const client = getCassandraClient();
    
    try {
      // First check if the keyspace exists
      const keyspaceExists = await this.checkKeyspaceExists(client, input.keyspaceName);
      if (!keyspaceExists) {
        return {
          success: false,
          error: `Keyspace '${input.keyspaceName}' not found.`,
          tables: []
        };
      }
      
      // Then get tables
      const tablesResult = await this.getTables(client, input.keyspaceName, !!input.includeViews);
      
      // No tables found
      if (!tablesResult.success || tablesResult.tables.length === 0) {
        return {
          success: true,
          message: `No ${input.includeViews ? 'tables or views' : 'tables'} found in keyspace '${input.keyspaceName}'.`,
          tables: []
        };
      }
      
      // Return formatted result with detailed information
      return {
        success: true,
        keyspace: input.keyspaceName,
        tables: tablesResult.tables,
        count: {
          total: tablesResult.tables.length,
          tables: tablesResult.tableCount || tablesResult.tables.length,
          views: tablesResult.viewCount || 0
        }
      };
    } catch (error: any) {
      console.error(`Error listing tables for keyspace ${input.keyspaceName}:`, error);
      
      // Try one final direct approach - useful on some older Cassandra versions
      try {
        const directQuery = `SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = ?`;
        const directResult = await client.execute(directQuery, [input.keyspaceName], { prepare: true });
        
        if (directResult.rows.length === 0) {
          return {
            success: false,
            error: `No tables found in keyspace '${input.keyspaceName}'.`,
            tables: []
          };
        }
        
        const tableNames = directResult.rows.map((row: any) => row.columnfamily_name);
        return {
          success: true,
          keyspace: input.keyspaceName,
          tables: tableNames.sort(),
          count: {
            total: tableNames.length,
            tables: tableNames.length,
            views: 0
          },
          note: "Retrieved using legacy system.schema_columnfamilies approach"
        };
      } catch (fallbackError: any) {
        console.error(`All fallback attempts failed for keyspace ${input.keyspaceName}:`, fallbackError);
        return {
          success: false,
          error: `Failed to list tables in keyspace '${input.keyspaceName}': ${error.message}`,
          tables: []
        };
      }
    }
  }
}

export default ListTablesTool; 
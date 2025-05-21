import { MCPTool } from "mcp-framework";
import { z } from "zod";
import { getCassandraClient } from "../cassandra-client.js";

interface ListIndexesInput {
  keyspaceName: string;
  tableName?: string;
}

class ListIndexesTool extends MCPTool<ListIndexesInput> {
  name = "cassandra_list_indexes";
  description = "Lists all secondary indexes and materialized views in a Cassandra keyspace, optionally filtered by table.";

  schema = {
    keyspaceName: {
      type: z.string().nonempty(),
      description: "The name of the keyspace for which to list indexes and views."
    },
    tableName: {
      type: z.string().optional(),
      description: "Optional table name to filter indexes by base table."
    }
  };

  async execute(input: ListIndexesInput) {
    const client = getCassandraClient();
    try {
      // Check if keyspace exists
      const keyspaceQuery = `SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?`;
      const keyspaceResult = await client.execute(keyspaceQuery, [input.keyspaceName], { prepare: true });
      
      if (keyspaceResult.rows.length === 0) {
        return { message: `Keyspace '${input.keyspaceName}' not found.` };
      }
      
      // Query for secondary indexes
      let indexQuery = `SELECT * FROM system_schema.indexes WHERE keyspace_name = ?`;
      let indexParams = [input.keyspaceName];
      
      if (input.tableName) {
        indexQuery += ` AND table_name = ?`;
        indexParams.push(input.tableName);
      }
      
      const indexResult = await client.execute(indexQuery, indexParams, { prepare: true });
      
      // Query for materialized views
      let viewQuery = `SELECT * FROM system_schema.views WHERE keyspace_name = ?`;
      let viewParams = [input.keyspaceName];
      
      if (input.tableName) {
        viewQuery += ` AND base_table_name = ?`;
        viewParams.push(input.tableName);
      }
      
      const viewResult = await client.execute(viewQuery, viewParams, { prepare: true });
      
      // Format results
      const indexes = indexResult.rows.map(idx => ({
        name: idx.index_name,
        table: idx.table_name,
        type: 'SECONDARY_INDEX',
        options: {
          target: idx.options?.target,
          indexClass: idx.options?.class_name,
          kind: idx.kind
        }
      }));
      
      const views = viewResult.rows.map(view => ({
        name: view.view_name,
        baseTable: view.base_table_name,
        type: 'MATERIALIZED_VIEW',
        includedColumns: view.included_columns,
        whereClause: view.where_clause
      }));
      
      // Combine results
      const result = {
        secondaryIndexes: indexes,
        materializedViews: views,
        baseTable: input.tableName,
        keyspace: input.keyspaceName,
        summary: {
          totalIndexes: indexes.length,
          totalViews: views.length
        }
      };
      
      if (indexes.length === 0 && views.length === 0) {
        const tableSpecificMessage = input.tableName ? 
          `No indexes or materialized views found for table '${input.keyspaceName}.${input.tableName}'.` :
          `No indexes or materialized views found in keyspace '${input.keyspaceName}'.`;
        return { message: tableSpecificMessage };
      }
      
      return result;
    } catch (error: any) {
      console.error(`Error listing indexes for keyspace ${input.keyspaceName}:`, error);
      return { error: `Failed to list indexes: ${error.message}` };
    }
  }
}

export default ListIndexesTool; 
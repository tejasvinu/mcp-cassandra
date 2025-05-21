import { MCPTool } from "mcp-framework";
import { z } from "zod";
import { getCassandraClient } from "../cassandra-client.js";
import { types } from 'cassandra-driver';

// Define complex schemas using zod
const whereClauseSchema = z.record(z.string(), z.any());
const orderBySchema = z.object({
  column: z.string().nonempty(),
  order: z.enum(['ASC', 'DESC'])
});

interface SelectRowsInput {
  keyspaceName: string;
  tableName: string;
  selectColumns?: string[]; // Array of column names to select. If empty or undefined, selects all ('*')
  whereClauses?: Record<string, any>; // Key-value pairs for WHERE clauses (ANDed together)
  orderBy?: { column: string; order: 'ASC' | 'DESC' } | Array<{ column: string; order: 'ASC' | 'DESC' }>; // Single or array of order by objects
  limit?: number;
  allowFiltering?: boolean;
  consistency?: 'ANY' | 'ONE' | 'TWO' | 'THREE' | 'QUORUM' | 'LOCAL_QUORUM' | 'EACH_QUORUM' | 'SERIAL' | 'LOCAL_SERIAL' | 'LOCAL_ONE' | 'ALL';
  fetchSize?: number;
  pageState?: string;
}

const getConsistencyLevel = (consistencyStr?: string): types.consistencies | undefined => {
    if (!consistencyStr) return undefined;
    const upperConsistencyStr = consistencyStr.toUpperCase();
    const mapping: { [key: string]: types.consistencies } = {
      ANY: types.consistencies.any, ONE: types.consistencies.one, TWO: types.consistencies.two,
      THREE: types.consistencies.three, QUORUM: types.consistencies.quorum, LOCAL_QUORUM: types.consistencies.localQuorum,
      EACH_QUORUM: types.consistencies.eachQuorum, SERIAL: types.consistencies.serial,
      LOCAL_SERIAL: types.consistencies.localSerial, LOCAL_ONE: types.consistencies.localOne, ALL: types.consistencies.all
    };
    return mapping[upperConsistencyStr];
}

class SelectRowsTool extends MCPTool<SelectRowsInput> {
  name = "cassandra_select_rows";
  description = "Selects rows from a Cassandra table with optional filtering, ordering, and limiting.";

  schema = {
    keyspaceName: { 
      type: z.string().nonempty(), 
      description: "Keyspace name." 
    },
    tableName: { 
      type: z.string().nonempty(), 
      description: "Table name." 
    },
    selectColumns: { 
      type: z.array(z.string().nonempty()).optional(), 
      description: "Optional array of column names to select. If omitted, selects all columns ('*')."
    },
    whereClauses: {
      type: z.record(z.string(), z.any()).optional(),
      description: "Optional map of column names to values for WHERE clauses. Conditions are ANDed. Example: { \"user_id\": \"123\", \"status\": \"active\" }"
    },
    orderBy: {
      type: z.union([
        z.object({
          column: z.string().nonempty(),
          order: z.enum(['ASC', 'DESC'])
        }),
        z.array(z.object({
          column: z.string().nonempty(),
          order: z.enum(['ASC', 'DESC'])
        }))
      ]).optional(),
      description: "Optional ordering. Can be a single {column, order} object or an array of them."
    },
    limit: { 
      type: z.number().int().positive().optional(), 
      description: "Optional LIMIT clause for the number of rows to return." 
    },
    allowFiltering: { 
      type: z.boolean().optional().default(false), 
      description: "Optional ALLOW FILTERING clause. Use with caution. Defaults to false." 
    },
    consistency: { 
      type: z.enum(['ANY', 'ONE', 'TWO', 'THREE', 'QUORUM', 'LOCAL_QUORUM', 'EACH_QUORUM', 'SERIAL', 'LOCAL_SERIAL', 'LOCAL_ONE', 'ALL']).optional(),
      description: "Optional consistency level (e.g., ONE, LOCAL_QUORUM). Defaults to LOCAL_ONE."
    },
    fetchSize: { 
      type: z.number().int().positive().optional(), 
      description: "Optional number of rows to fetch per page." 
    },
    pageState: { 
      type: z.string().optional(), 
      description: "Optional page state for pagination (hex string)." 
    }
  };

  // Helper to safely format data for return
  private safelyFormatResult(result: types.ResultSet) {
    try {
      return {
        rows: result.rows.map(row => {
          // Convert row to plain object to avoid serialization issues
          const obj: Record<string, any> = {};
          for (const prop in row) {
            try {
              // Handle Map objects specially
              if (row[prop] instanceof Map) {
                const mapObj: Record<string, any> = {};
                row[prop].forEach((value: any, key: any) => {
                  mapObj[key.toString()] = value;
                });
                obj[prop] = mapObj;
              } else if (Buffer.isBuffer(row[prop])) {
                // Handle Buffer objects (e.g., BLOB data)
                obj[prop] = row[prop].toString('hex');
              } else if (row[prop] instanceof Date) {
                // Handle Date objects
                obj[prop] = row[prop].toISOString();
              } else if (row[prop] instanceof Set) {
                // Handle Set objects
                obj[prop] = Array.from(row[prop]);
              } else if (Array.isArray(row[prop])) {
                // Handle Array objects
                obj[prop] = [...row[prop]];
              } else {
                // Regular value
                obj[prop] = row[prop];
              }
            } catch (err: any) {
              // If we can't serialize a property, include an error message
              obj[prop] = `[Error serializing: ${err.message}]`;
            }
          }
          return obj;
        }),
        columns: result.columns?.map((col: any) => {
          try {
            return {
              name: col.name,
              type: col.type?.name || (typeof col.type === 'string' ? col.type : 'unknown')
            };
          } catch (err: any) {
            return { name: col.name, type: 'unknown' };
          }
        }),
        pageState: result.pageState ? Buffer.from(result.pageState).toString('hex') : undefined,
        warnings: result.info?.warnings || [],
        info: {
          queriedHost: result.info?.queriedHost,
          triedHosts: result.info?.triedHosts,
          achievedConsistency: result.info?.achievedConsistency,
        }
      };
    } catch (error: any) {
      console.error("Error formatting result:", error);
      return { error: `Error formatting result: ${error.message}` };
    }
  }

  async execute(input: SelectRowsInput) {
    const client = getCassandraClient();
    const fqTableName = `${input.keyspaceName}.${input.tableName}`;
    const params: any[] = [];

    // Build the CQL query
    const selectExpression = (input.selectColumns && input.selectColumns.length > 0) 
        ? input.selectColumns.join(', ') 
        : '*';

    let cql = `SELECT ${selectExpression} FROM ${fqTableName}`;

    // Add WHERE clauses
    if (input.whereClauses && Object.keys(input.whereClauses).length > 0) {
      const conditions = Object.entries(input.whereClauses).map(([column, value]) => {
        params.push(value);
        return `${column} = ?`; // Using ? as placeholder
      });
      cql += ` WHERE ${conditions.join(' AND ')}`;
    }

    // Add ORDER BY clauses
    if (input.orderBy) {
      const orderClauses = Array.isArray(input.orderBy) ? input.orderBy : [input.orderBy];
      if (orderClauses.length > 0) {
        const orderByStrings = orderClauses.map(ob => `${ob.column} ${ob.order}`);
        cql += ` ORDER BY ${orderByStrings.join(', ')}`;
      }
    }

    // Add LIMIT clause
    if (input.limit) {
      cql += ` LIMIT ${input.limit}`;
    }

    // Add ALLOW FILTERING clause
    if (input.allowFiltering) {
      cql += ' ALLOW FILTERING';
    }
    
    cql += ';';

    // Get consistency level
    const consistency = getConsistencyLevel(input.consistency) ?? types.consistencies.localOne;
    
    try {
      const result: types.ResultSet = await client.execute(cql, params, {
        prepare: true,
        consistency: consistency,
        fetchSize: input.fetchSize,
        pageState: input.pageState ? Buffer.from(input.pageState, 'hex') : undefined
      });

      // Format and return the result safely
      return this.safelyFormatResult(result);
    } catch (error: any) {
      console.error(`Error selecting rows from ${fqTableName}: ${cql} with params ${JSON.stringify(params)}`, error);
      
      // Return detailed error information
      return {
        error: {
          message: `Failed to select rows: ${error.message}`,
          query: cql,
          parameters: params,
          code: error.code,
          reason: error.reason || undefined,
          hint: this.getErrorHint(error)
        }
      };
    }
  }

  // Helper to provide better error hints
  private getErrorHint(error: any): string {
    const message = error.message?.toLowerCase() || '';
    
    if (message.includes('allow filtering')) {
      return "This query requires ALLOW FILTERING. Set allowFiltering to true, but note this may be expensive on large tables.";
    }
    
    if (message.includes('unconfigured table')) {
      return "The specified table does not exist. Check keyspaceName and tableName parameters.";
    }
    
    if (message.includes('no keyspace has been specified')) {
      return "No keyspace was specified. Ensure keyspaceName is provided and valid.";
    }
    
    if (message.includes('undefined column name')) {
      return "One or more column names are invalid. Check the selectColumns and whereClauses parameters.";
    }
    
    if (message.includes('order by')) {
      return "There is an issue with the ORDER BY clause. Remember that Cassandra can only order by clustering columns.";
    }
    
    if (message.includes('permission')) {
      return "Permission denied. Check that your user has SELECT privileges on this table.";
    }
    
    return "Check your query syntax and make sure the table exists with the specified columns.";
  }
}

export default SelectRowsTool; 
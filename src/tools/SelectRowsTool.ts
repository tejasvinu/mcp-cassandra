import { MCPTool } from "mcp-framework";
import { z } from "zod";
import { getCassandraClient } from "../cassandra-client.js";
import { types } from 'cassandra-driver';

const whereClauseSchema = z.record(z.any()); // Example: { columnA: 'valueA', columnB: 123 }
const orderBySchema = z.object({
  column: z.string().nonempty(),
  order: z.enum(['ASC', 'DESC'])
});

interface SelectRowsInput {
  keyspaceName: string;
  tableName: string;
  selectColumns?: string[]; // Array of column names to select. If empty or undefined, selects all ('*')
  whereClauses?: z.infer<typeof whereClauseSchema>; // Key-value pairs for WHERE clauses (ANDed together)
  orderBy?: z.infer<typeof orderBySchema> | Array<z.infer<typeof orderBySchema>>; // Single or array of order by objects
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
    keyspaceName: { type: z.string().nonempty(), description: "Keyspace name." },
    tableName: { type: z.string().nonempty(), description: "Table name." },
    selectColumns: { 
      type: z.array(z.string().nonempty()).optional(), 
      description: "Optional array of column names to select. If omitted, selects all columns ('*')."
    },
    whereClauses: {
      type: whereClauseSchema.optional(),
      description: "Optional map of column names to values for WHERE clauses. Conditions are ANDed. Example: { \"user_id\": \"123\", \"status\": \"active\" }"
    },
    orderBy: {
        type: z.union([orderBySchema, z.array(orderBySchema)]).optional(),
        description: "Optional ordering. Can be a single {column, order} object or an array of them."
    },
    limit: { type: z.number().int().positive().optional(), description: "Optional LIMIT clause for the number of rows to return." },
    allowFiltering: { type: z.boolean().optional().default(false), description: "Optional ALLOW FILTERING clause. Use with caution. Defaults to false." },
    consistency: { 
        type: z.enum(['ANY', 'ONE', 'TWO', 'THREE', 'QUORUM', 'LOCAL_QUORUM', 'EACH_QUORUM', 'SERIAL', 'LOCAL_SERIAL', 'LOCAL_ONE', 'ALL']).optional(),
        description: "Optional consistency level (e.g., ONE, LOCAL_QUORUM). Defaults to LOCAL_ONE."
    },
    fetchSize: { type: z.number().int().positive().optional(), description: "Optional number of rows to fetch per page." },
    pageState: { type: z.string().optional(), description: "Optional page state for pagination (hex string)." }
  };

  async execute(input: SelectRowsInput) {
    const client = getCassandraClient();
    const fqTableName = `${input.keyspaceName}.${input.tableName}`;
    const params: any[] = [];
    let paramIndex = 1; // if Cassandra driver uses ? instead of $1, $2 style placeholders, this isn't needed.

    const selectExpression = (input.selectColumns && input.selectColumns.length > 0) 
        ? input.selectColumns.join(', ') 
        : '*';

    let cql = `SELECT ${selectExpression} FROM ${fqTableName}`;

    if (input.whereClauses && Object.keys(input.whereClauses).length > 0) {
      const conditions = Object.entries(input.whereClauses).map(([column, value]) => {
        params.push(value);
        return `${column} = ?`; // Using ? as placeholder
      });
      cql += ` WHERE ${conditions.join(' AND ')}`;
    }

    if (input.orderBy) {
        const orderClauses = Array.isArray(input.orderBy) ? input.orderBy : [input.orderBy];
        if (orderClauses.length > 0) {
            const orderByStrings = orderClauses.map(ob => `${ob.column} ${ob.order}`);
            cql += ` ORDER BY ${orderByStrings.join(', ')}`;
        }
    }

    if (input.limit) {
      cql += ` LIMIT ${input.limit}`;
    }

    if (input.allowFiltering) {
      cql += ' ALLOW FILTERING';
    }
    cql += ';';

    const consistency = getConsistencyLevel(input.consistency) ?? types.consistencies.localOne;
    
    try {
      const result: types.ResultSet = await client.execute(cql, params, {
        prepare: true,
        consistency: consistency,
        fetchSize: input.fetchSize,
        pageState: input.pageState ? Buffer.from(input.pageState, 'hex') : undefined
      });

      return {
        rows: result.rows,
        columns: result.columns?.map((col: any) => ({ name: col.name, type: col.type.name })),
        pageState: result.pageState ? Buffer.from(result.pageState).toString('hex') : undefined,
        warnings: result.info.warnings,
        info: result.info
      };
    } catch (error: any) {
      console.error(`Error selecting rows from ${fqTableName}: ${cql} with params ${JSON.stringify(params)}`, error);
      return `Failed to select rows: ${error.message}`;
    }
  }
}

export default SelectRowsTool; 
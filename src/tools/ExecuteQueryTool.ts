import { MCPTool } from "mcp-framework";
import { z } from "zod";
import { getCassandraClient } from "../cassandra-client.js";
import { types } from 'cassandra-driver';

interface ExecuteQueryInput {
  query: string;
  params?: any[];
  consistency?: 'ANY' | 'ONE' | 'TWO' | 'THREE' | 'QUORUM' | 'LOCAL_QUORUM' | 'EACH_QUORUM' | 'SERIAL' | 'LOCAL_SERIAL' | 'LOCAL_ONE' | 'ALL';
  fetchSize?: number;
  pageState?: string; 
}

// Helper to convert consistency string to enum value
const getConsistencyLevel = (consistencyStr?: string): types.consistencies | undefined => {
  if (!consistencyStr) return undefined;
  const upperConsistencyStr = consistencyStr.toUpperCase();
  // Mapping common string representations to enum values
  // This can be expanded as needed
  const mapping: { [key: string]: types.consistencies } = {
    ANY: types.consistencies.any,
    ONE: types.consistencies.one,
    TWO: types.consistencies.two,
    THREE: types.consistencies.three,
    QUORUM: types.consistencies.quorum,
    LOCAL_QUORUM: types.consistencies.localQuorum,
    EACH_QUORUM: types.consistencies.eachQuorum,
    SERIAL: types.consistencies.serial,
    LOCAL_SERIAL: types.consistencies.localSerial,
    LOCAL_ONE: types.consistencies.localOne,
    ALL: types.consistencies.all
  };
  return mapping[upperConsistencyStr];
}

class ExecuteQueryTool extends MCPTool<ExecuteQueryInput> {
  name = "cassandra_execute_query";
  description = "Executes a CQL query against the Cassandra cluster.";

  schema = {
    query: {
      type: z.string(),
      description: "The CQL query string to execute."
    },
    params: {
      type: z.array(z.any()).optional(),
      description: "Optional array of parameters for the CQL query (for prepared statements)."
    },
    consistency: {
        type: z.enum(['ANY', 'ONE', 'TWO', 'THREE', 'QUORUM', 'LOCAL_QUORUM', 'EACH_QUORUM', 'SERIAL', 'LOCAL_SERIAL', 'LOCAL_ONE', 'ALL']).optional(),
        description: "Optional consistency level (e.g., ONE, LOCAL_QUORUM, QUORUM). Defaults to LOCAL_ONE if not specified."
    },
    fetchSize: {
        type: z.number().int().positive().optional(),
        description: "Optional number of rows to fetch per page."
    },
    pageState: {
        type: z.string().optional(),
        description: "Optional page state for pagination."
    }
  };

  async execute(input: ExecuteQueryInput) {
    try {
      const client = getCassandraClient();
      const consistency = getConsistencyLevel(input.consistency) ?? types.consistencies.localOne;
      
      const result: types.ResultSet = await client.execute(input.query, input.params, {
        prepare: true, // Automatically prepare queries
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
      console.error(`Error executing query: ${input.query}`, error);
      return `Failed to execute query: ${error.message}`;
    }
  }
}

export default ExecuteQueryTool; 
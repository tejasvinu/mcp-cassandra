import { MCPTool } from "mcp-framework";
import { z } from "zod";
import { getCassandraClient } from "../cassandra-client.js";

interface ListKeyspacesInput {
  includeSystemKeyspaces?: boolean;
}

class ListKeyspacesTool extends MCPTool<ListKeyspacesInput> {
  name = "cassandra_list_keyspaces";
  description = "Lists all available keyspaces in the Cassandra cluster. System keyspaces can be included optionally.";

  schema = {
    includeSystemKeyspaces: {
      type: z.boolean().optional().default(false),
      description: "Whether to include system keyspaces in the results (default: false)"
    }
  };

  async execute(input: ListKeyspacesInput) {
    const client = getCassandraClient();
    try {
      // client.metadata.keyspaces is an object where keys are keyspace names
      const keyspacesObject = client.metadata.keyspaces;
      const allKeyspaceNames = Object.keys(keyspacesObject);
      
      let keyspaces = allKeyspaceNames;
      
      // Filter out system keyspaces if not explicitly requested
      if (!input.includeSystemKeyspaces) {
        keyspaces = allKeyspaceNames.filter(
          name => !name.startsWith('system') && name !== 'dse_system'
        );
      }

      if (keyspaces.length === 0) {
        return "No keyspaces found.";
      }
      
      return keyspaces.sort();
    } catch (error: any) {
      console.error("Error listing keyspaces:", error);
      return `Failed to list keyspaces: ${error.message}`;
    }
  }
}

export default ListKeyspacesTool; 
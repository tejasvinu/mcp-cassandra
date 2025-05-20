import { MCPTool } from "mcp-framework";
import { z } from "zod";
import { disconnectFromCassandra } from "../cassandra-client.js";

class DisconnectTool extends MCPTool<Record<string, never>> {
  name = "cassandra_disconnect";
  description = "Disconnects from the Cassandra cluster.";

  schema = {}; // No input parameters

  async execute() {
    try {
      await disconnectFromCassandra();
      return "Successfully disconnected from Cassandra.";
    } catch (error: any) {
      return `Failed to disconnect: ${error.message}`;
    }
  }
}

export default DisconnectTool; 
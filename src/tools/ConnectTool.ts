import { MCPTool } from "mcp-framework";
import { z } from "zod";
import { connectToCassandra, CassandraConnectionParams } from "../cassandra-client.js";

class ConnectTool extends MCPTool<CassandraConnectionParams> {
  name = "cassandra_connect";
  description = "Connects to a Cassandra cluster.";

  schema = {
    contactPoints: {
      type: z.array(z.string()),
      description: "An array of contact points (IP addresses or hostnames) for the Cassandra cluster.",
      items: {
        type: "string"
      }
    },
    localDataCenter: {
      type: z.string(),
      description: "The name of the local data center."
    },
    keyspace: {
      type: z.string().optional(),
      description: "Optional default keyspace to use for the connection."
    },
    username: {
      type: z.string().optional(),
      description: "Optional username for Cassandra authentication."
    },
    password: {
      type: z.string().optional(),
      description: "Optional password for Cassandra authentication."
    },
    port: {
      type: z.number().optional(),
      description: "Optional connection port (default: 9042)."
    }
  };

  async execute(input: CassandraConnectionParams) {
    try {
      await connectToCassandra(input);
      return { message: "Successfully connected to Cassandra." };
    } catch (error: any) {
      return { message: `Failed to connect: ${error.message}` };
    }
  }
}

export default ConnectTool; 
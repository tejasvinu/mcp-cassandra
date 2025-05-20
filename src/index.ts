import { MCPServer } from "mcp-framework";

// The MCPServer constructor should automatically discover and load tools
// from the ./tools directory based on the framework's conventions.
const server = new MCPServer();

// The server.start() method will initialize and start the MCP server
// with the loaded tools.
server.start();
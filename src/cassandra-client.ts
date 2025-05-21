import { Client, auth, types, QueryOptions } from 'cassandra-driver';

let client: Client | null = null;
let currentConnectionConfig: CassandraConnectionParams | null = null;

export interface CassandraConnectionParams {
  contactPoints: string[];
  localDataCenter: string;
  keyspace?: string;
  username?: string;
  password?: string;
  port?: number;
  queryOptions?: {
    consistency?: types.consistencies;
    serialConsistency?: types.consistencies;
    fetchSize?: number;
    prepare?: boolean;
    // Explicitly exclude timeout as it's not in the QueryOptions type
    // requestTimeout?: number; // Use this at the execution level instead
  };
  socketOptions?: {
    connectTimeout?: number;
    readTimeout?: number;
    keepAlive?: boolean;
  };
  pooling?: {
    coreConnectionsPerHost?: { [key: number]: number };
    maxConnectionsPerHost?: { [key: number]: number };
    heartBeatInterval?: number;
  };
}

export interface ConnectionStatus {
  isConnected: boolean;
  config?: {
    contactPoints: string[];
    keyspace?: string;
    localDataCenter: string;
  };
  connectionTime?: Date;
}

// Track connection time for debugging
let connectionTime: Date | undefined = undefined;

/**
 * Get the current Cassandra client instance
 * @returns A connected Cassandra client
 * @throws Error if client is not initialized
 */
export const getCassandraClient = (): Client => {
  if (!client) {
    throw new Error('Cassandra client is not initialized. Call connect first.');
  }
  return client;
};

/**
 * Check if the client is currently connected to Cassandra
 * @returns Connection status information
 */
export const getConnectionStatus = (): ConnectionStatus => {
  if (!client) {
    return { isConnected: false };
  }
  
  return {
    isConnected: true,
    config: currentConnectionConfig ? {
      contactPoints: currentConnectionConfig.contactPoints,
      keyspace: currentConnectionConfig.keyspace,
      localDataCenter: currentConnectionConfig.localDataCenter
    } : undefined,
    connectionTime: connectionTime
  };
};

/**
 * Connect to Cassandra database
 * @param params Connection parameters
 * @returns Promise that resolves when connection is established
 * @throws Error if connection fails
 */
export const connectToCassandra = async (params: CassandraConnectionParams): Promise<void> => {
  // If already connected, close existing connection
  if (client) {
    console.log('Already connected to Cassandra. Closing existing connection before creating a new one.');
    try {
      await client.shutdown();
    } catch (error) {
      console.warn('Warning: Error shutting down previous connection:', error);
      // Continue anyway to establish new connection
    }
  }

  // Set up authentication if provided
  const authProvider = params.username && params.password ?
    new auth.PlainTextAuthProvider(params.username, params.password) : undefined;

  // Default port is 9042
  const port = params.port || 9042;

  try {
    // Create new client with provided parameters
    client = new Client({
      contactPoints: params.contactPoints,
      localDataCenter: params.localDataCenter,
      keyspace: params.keyspace,
      authProvider: authProvider,
      protocolOptions: {
        port
      },
      queryOptions: {
        consistency: params.queryOptions?.consistency ?? types.consistencies.localOne,
        serialConsistency: params.queryOptions?.serialConsistency,
        fetchSize: params.queryOptions?.fetchSize ?? 1000,
        prepare: params.queryOptions?.prepare ?? true,
        // timeout property removed as it's not in the QueryOptions type
      },
      socketOptions: {
        connectTimeout: params.socketOptions?.connectTimeout,
        ...(typeof params.socketOptions?.readTimeout === 'number' ? { readTimeout: params.socketOptions.readTimeout } : {}),
        keepAlive: params.socketOptions?.keepAlive ?? true
      },
      pooling: params.pooling
    });

    // Attempt to connect
    await client.connect();
    console.log('Successfully connected to Cassandra at', params.contactPoints.join(','));
    
    // Store successful connection details for reference
    currentConnectionConfig = { ...params };
    connectionTime = new Date();
    
    // Check keyspace 
    if (!params.keyspace) {
      console.log('No keyspace specified. You will need to specify keyspace in queries or switch keyspace later.');
    } else {
      // Verify keyspace exists
      try {
        await client.execute(`USE ${params.keyspace}`);
        console.log(`Successfully connected to keyspace: ${params.keyspace}`);
      } catch (error: any) {
        console.error(`Connected to Cassandra, but couldn't use keyspace ${params.keyspace}:`, error.message);
        // We won't throw an error here because connection is valid, just keyspace selection failed
      }
    }
  } catch (error: any) {
    client = null;
    currentConnectionConfig = null;
    connectionTime = undefined;
    console.error('Failed to connect to Cassandra:', error);
    
    // Enhance error message with connection details
    let errorMessage = `Failed to connect to Cassandra: ${error.message}`;
    if (error.code === 'ECONNREFUSED') {
      errorMessage += ` - Check that Cassandra is running at ${params.contactPoints.join(',')}:${port} and network connectivity is available`;
    } else if (error.code === 'ETIMEDOUT') {
      errorMessage += ` - Connection timed out. Check network connectivity and firewall settings`;
    } else if (error.message?.includes('authentication')) {
      errorMessage += ` - Authentication failed. Check username and password`;
    }
    
    throw new Error(errorMessage);
  }
};

/**
 * Disconnect from Cassandra database
 * @returns Promise that resolves when disconnection is complete
 */
export const disconnectFromCassandra = async (): Promise<void> => {
  if (client) {
    try {
      await client.shutdown();
      console.log('Successfully disconnected from Cassandra.');
    } catch (error: any) {
      console.error('Failed to disconnect from Cassandra:', error);
      throw new Error(`Failed to disconnect from Cassandra: ${error.message}`);
    } finally {
      client = null;
      currentConnectionConfig = null;
      connectionTime = undefined;
    }
  } else {
    console.log('Not connected to Cassandra.');
  }
};

/**
 * Change the active keyspace
 * @param keyspace The keyspace to switch to
 * @returns Promise that resolves when keyspace is changed
 * @throws Error if not connected or keyspace doesn't exist
 */
export const useKeyspace = async (keyspace: string): Promise<void> => {
  if (!client) {
    throw new Error('Cassandra client is not initialized. Call connect first.');
  }
  
  try {
    await client.execute(`USE ${keyspace}`);
    
    // Update current configuration
    if (currentConnectionConfig) {
      currentConnectionConfig.keyspace = keyspace;
    }
    
    console.log(`Successfully switched to keyspace: ${keyspace}`);
  } catch (error: any) {
    console.error(`Failed to use keyspace ${keyspace}:`, error);
    throw new Error(`Failed to use keyspace ${keyspace}: ${error.message}`);
  }
}; 
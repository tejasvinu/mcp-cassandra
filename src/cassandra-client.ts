import { Client, auth } from 'cassandra-driver';

let client: Client | null = null;

export interface CassandraConnectionParams {
  contactPoints: string[];
  localDataCenter: string;
  keyspace?: string;
  username?: string;
  password?: string;
}

export const getCassandraClient = (): Client => {
  if (!client) {
    throw new Error('Cassandra client is not initialized. Call connect first.');
  }
  return client;
};

export const connectToCassandra = async (params: CassandraConnectionParams): Promise<void> => {
  if (client) {
    console.log('Already connected to Cassandra. Closing existing connection before creating a new one.');
    await client.shutdown();
  }

  const authProvider = params.username && params.password ?
    new auth.PlainTextAuthProvider(params.username, params.password) : undefined;

  client = new Client({
    contactPoints: params.contactPoints,
    localDataCenter: params.localDataCenter,
    keyspace: params.keyspace,
    authProvider: authProvider,
    protocolOptions: {
      port: 9042 // Default Cassandra port
    },
    queryOptions: { consistency: 1 } // Default to LOCAL_ONE, can be configured per query
  });

  try {
    await client.connect();
    console.log('Successfully connected to Cassandra.');
  } catch (error) {
    client = null; // Reset client on connection failure
    console.error('Failed to connect to Cassandra:', error);
    throw error; // Re-throw to be handled by the tool
  }
};

export const disconnectFromCassandra = async (): Promise<void> => {
  if (client) {
    try {
      await client.shutdown();
      console.log('Successfully disconnected from Cassandra.');
    } catch (error) {
      console.error('Failed to disconnect from Cassandra:', error);
      throw error;
    } finally {
      client = null;
    }
  } else {
    console.log('Not connected to Cassandra.');
  }
}; 
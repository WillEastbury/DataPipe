using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using Newtonsoft.Json;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Linq;

namespace ChangeWriter
{
    public class ProgramWriter
    {
        private static string sqlConnectionString = Environment.GetEnvironmentVariable("sqlconn");
        private static string ServiceBusConnectionString = Environment.GetEnvironmentVariable("sbconnwri");
      
        private const string QueueName = "Ingress";
        private const string Path = "waypoint.txt";

        static IQueueClient queueClient = new QueueClient(ServiceBusConnectionString, QueueName);
        static SqlConnection connection = new SqlConnection(sqlConnectionString);

        static List<string> SyncWhat = new List<string> { "SampleTable1", "SampleTable2", "SampleTable3", "SampleTable4" };

        // Singlethreaded - to run multiple tables in parallel - use a ConcurrentDictionary.
        static Dictionary<string, int> lastSync = new Dictionary<string, int>();

        // Following sample code is based on
        // https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-dotnet-get-started-with-queues
        // and
        // https://docs.microsoft.com/en-us/azure/sql-database/sql-database-connect-query-dotnet-core


        static async Task Main(string[] args)
        {

            // -- Writer --
            // Read the last waypoint numbers - or default is start from zero if it doesn't exist

            if (File.Exists(Path))
            {
                lastSync = JsonConvert.DeserializeObject<Dictionary<string, int>>(File.ReadAllText(Path));
            }

            Console.WriteLine($"{ DateTime.Now} :: Application Started");

            // Run forever, this will be a webjob.
            while (1 == 1)
            {

                try
                {

                    // Connect to the SQL Instance
                    // If this is the first run, open the connection - the webjob infra will auto-restart on any transient errors at the service level.
                    if (connection.State != ConnectionState.Open) connection.Open();

                    // Connect to the service bus
                    string sql;

                    // Read the base tables
                    foreach (string table in SyncWhat)
                    {
                        int rowcount = 0;
                        int priorsync = 0;

                        if (!lastSync.ContainsKey(table))
                        {
                            lastSync.Add(table, 0);

                        }

                        priorsync = lastSync[table];

                        sql = $"SELECT *, CONVERT(bigint,rowver) as RowVersion FROM {table} WHERE CONVERT(bigint,rowver) > {lastSync[table]} ORDER BY CONVERT(bigint,rowver)";

                        // Batch them, compress them and send them to the Service Bus
                        TransferBatch tb = new TransferBatch
                        {
                            Values = new List<object[]>()
                        };

                        using (SqlCommand command = new SqlCommand(sql, connection))
                        {
                            using (SqlDataReader reader = command.ExecuteReader())
                            {
                                // Loop for each message
                                while (reader.Read())
                                {
                                    rowcount++;
                                    // Setup the Schema values object and get the column data for the row.

                                    var schema = reader.GetColumnSchema();
                                    tb.Schema = schema
                                        .Select(r => new TransferSchemaEntry()
                                        {
                                            ColumnName = r.ColumnName,
                                            DataTypeName = r.DataTypeName,
                                            IsIdentity = (bool)r.IsIdentity
                                        });

                                    tb.IdColumn = schema
                                        .Where(e => e.IsIdentity == true)
                                        .Select(s => s.ColumnName).FirstOrDefault();

                                    int columns = tb.Schema.Count();

                                    // Add the actual change data
                                    object[] row = new object[columns];
                                    reader.GetValues(row);
                                    tb.Values.Add(row);

                                    // Is this record past the end of the bookmark
                                    if (int.Parse(reader["RowVersion"].ToString()) > lastSync[table])
                                    {

                                        lastSync[table] = int.Parse(reader["RowVersion"].ToString());

                                    }

                                }

                                // Create a new message to send to the queue.
                                byte[] bytedata = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(tb));

                                try
                                {
                                    // Compress and send the encoded bytedata
                                    Message msg = new Message(Compress(bytedata));

                                    // Add metadata for the receiver
                                    msg.UserProperties.Add("Table", table);
                                    msg.UserProperties.Add("ChangeFrom", priorsync);
                                    msg.UserProperties.Add("ChangeTo", lastSync[table]);
                                    msg.UserProperties.Add("Changes", rowcount);

                                    // Write the body of the message to the console.
                                    Console.WriteLine($"Detected {rowcount} Changes in {table}");

                                    if (rowcount > 0)
                                    {
                                        await queueClient.SendAsync(msg);
                                        Console.WriteLine($"{DateTime.Now} :: Streamed {rowcount} messages");
                                    }
                                }
                                catch (Exception exception)
                                {
                                    Console.WriteLine($"{DateTime.Now} :: Exception From the ServiceBus Send: {exception.Message}");
                                }
                            }
                        }

                        // Don't batter the source SQL instance with individual requests, retrieve in batches, 100ms apart
                        await Task.Delay(100);
                    }

                }
                catch (SqlException e)
                {
                    Console.WriteLine($"{DateTime.Now} ::Exception From the SqlServer Client: { e.Message}");
                }

                // Success ! Commit the last Sync numbers to the disk 
                File.WriteAllText(Path, JsonConvert.SerializeObject(lastSync));

                // Give a 5 second backoff time before the next sync run, this will result in more efficient batches
                Console.WriteLine($"{DateTime.Now} ::Sync Run Complete - Waiting 5s");
                await Task.Delay(5000);
            }
        }

        public class TransferBatch
        {

            public IEnumerable<TransferSchemaEntry> Schema { get; set; } = new List<TransferSchemaEntry>();

            public string IdColumn { set; get; } = "";

            public IList<object[]> Values { get; set; } = new List<object[]>();

        }

        public class TransferSchemaEntry
        {

            public string ColumnName { get; set; }
            public string DataTypeName { get; set; }
            public bool IsIdentity { get; set; }

        }

        static byte[] Compress(byte[] data)
        {
            using (var compressedStream = new MemoryStream())
            using (var zipStream = new GZipStream(compressedStream, CompressionMode.Compress))
            {
                zipStream.Write(data, 0, data.Length);
                zipStream.Close();
                return compressedStream.ToArray();
            }
        }
    }
}
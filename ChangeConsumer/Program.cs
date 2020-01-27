using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ChangeConsumer
{
    public class ProgramConsumer
    {
        public static string inboundsqlConnectionString = Environment.GetEnvironmentVariable("sqlconn");
        public static string ServiceBusConnectionString = Environment.GetEnvironmentVariable("sbconncon");
        private const string QueueName = "Ingress";

        public static IQueueClient queueClient;
        public static SqlConnection connection;

        // Based on sample code from https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-dotnet-get-started-with-queues#receive-messages-from-the-queue
        // and
        // https://docs.microsoft.com/en-us/azure/sql-database/sql-database-connect-query-dotnet-core

        public static async Task Main(string[] args)
        {

            queueClient = new QueueClient(ServiceBusConnectionString, QueueName);
            connection = new SqlConnection(inboundsqlConnectionString);

            // Connect to the SQL Instance
            // If this is the first run, open the connection - the infra should be able to auto-restart on any errors at the service level.
            if (connection.State != ConnectionState.Open) connection.Open();

                // Register the queue message handler and receive messages in a loop
                RegisterOnMessageHandlerAndReceiveMessages();

                Console.WriteLine("Enter to Exit Receiver"); 
                Console.ReadLine();

                await queueClient.CloseAsync();

        }

        static void RegisterOnMessageHandlerAndReceiveMessages()
        {
            // Configure the message handler options in terms of exception handling, number of concurrent messages to deliver, etc.
            var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
            {
                // Maximum number of concurrent calls to the callback ProcessMessagesAsync(), set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 4,

                // Indicates whether the message pump should automatically complete the messages after returning from user callback.
                // False below indicates the complete operation is handled by the user callback as in ProcessMessagesAsync().
                AutoComplete = false
                
            };

            // Register the function that processes messages.
            queueClient.RegisterMessageHandler(ProcessMessagesAsync, messageHandlerOptions);
        }

        static async Task ProcessMessagesAsync(Message message, CancellationToken token)
        {

            // Extract, Decompress, decode, then deserialize the message
            byte[] inbound = message.Body;
            int records = int.Parse(message.UserProperties["Changes"].ToString());
            byte[] decompressed = Decompress(inbound);
            string decoded = Encoding.UTF8.GetString(decompressed);
            dynamic deserialized = JsonConvert.DeserializeObject(decoded);

            // Optional suffix for testing to replicate back to the same database
            string suffix = "a";

            // Grab any properties we need 
            string table = message.UserProperties["Table"].ToString() + suffix;
            string BatchStart = message.UserProperties["ChangeFrom"].ToString();
            string BatchFinish = message.UserProperties["ChangeTo"].ToString();

            // Process the message.
            Console.WriteLine($"Received batch: SequenceNumber: {message.SystemProperties.SequenceNumber} >> {decoded}");
            Console.WriteLine($"Received {records} Records from table: {table} in batch ");
            string idkey = deserialized.IdColumn;

            foreach (JArray record in deserialized.Values)
            {
                // Do your merge here
                Console.WriteLine($"Received Record from table: {table} : >> {record} >> Merging");
                
                SqlCommand sqlcom = new SqlCommand("", connection);

                List<string> sbset = new List<string>();
                List<string> sbinsert = new List<string>();

                List<SqlParameter> sqlparams = new List<SqlParameter>();
                List<string> sqlparamlist = new List<string>();
                List<string> sqlsetlist = new List<string>();

                for (int i = 0; i < deserialized.Schema.Count; i++)
                {
                    Console.WriteLine($"Processing {deserialized.Schema[i].ColumnName} - {deserialized.Schema[i].DataTypeName} - {deserialized.Schema[i].IsIdentity}");
                    if (deserialized.Schema[i].ColumnName != "rowver" && deserialized.Schema[i].ColumnName != "RowVersion" && deserialized.Schema[i].IsIdentity == "false")
                    {

                        sbset.Add($"{deserialized.Schema[i].ColumnName} = @p{i}");
                        sbinsert.Add($"{deserialized.Schema[i].ColumnName}");
                        SqlParameter tsp = new SqlParameter("@p" + i, Enum.Parse(typeof(SqlDbType), deserialized.Schema[i].DataTypeName.ToString(), true));
                        tsp.Value = record[i].ToString();

                        sqlparams.Add(tsp) ;
                        sqlparamlist.Add("@p" + i); 

                        Console.WriteLine($"@p{i} param added as {deserialized.Schema[i].DataTypeName}");

                    }

                    if (deserialized.Schema[i].ColumnName == "RowVersion")
                    {
                        SqlParameter tsp = new SqlParameter("@prowver", SqlDbType.Int);
                        tsp.Value = record[i].ToString();
                        sqlparams.Add(tsp);

                    }

                    if (deserialized.Schema[i].IsIdentity == true)
                    {
                        SqlParameter tsp = new SqlParameter("@prowid", SqlDbType.Int);
                        tsp.Value = record[i].ToString();
                        sqlparams.Add(tsp);

                    }
                }
               
                sqlcom.CommandText = $"IF ((SELECT COUNT(*) as Rows FROM {table} WHERE RowID = @prowid) = 0)\r\n" +
                $"BEGIN " +
                $"  INSERT INTO {table} (RowId, {string.Join(",", sbinsert.ToArray())}, RowVer) VALUES (@prowid, {string.Join(", ", sqlparamlist.ToArray())}, @prowver);\r\n" +
                $"END \r\n" +
                $"ELSE \r\n" +
                $"BEGIN \r\n" +
                $" UPDATE {table} \r\n" +
                $" SET  \r\n" +
                $"      {string.Join(", ", sbset.ToArray())},\r\n" +
                "      RowVer = @prowver\r\n" +
                " WHERE RowId = @prowid \r\n" +
                "END; \r\n";

                sqlcom.Parameters.AddRange(sqlparams.ToArray());
                sqlcom.ExecuteNonQuery();
                               
                Console.WriteLine("EXECUTED: row");
            }

            Console.WriteLine("BATCH COMPLETED");

            // Complete the message so that it is not received again.
            // This can be done only if the queue Client is created in ReceiveMode.PeekLock mode (which is the default).
            await queueClient.CompleteAsync(message.SystemProperties.LockToken);
           
        }
                
        // Use this handler to examine the exceptions received on the message pump.
        static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }

        static byte[] Decompress(byte[] data)
        {
            using (var compressedStream = new MemoryStream(data))
            using (var zipStream = new GZipStream(compressedStream, CompressionMode.Decompress))
            using (var resultStream = new MemoryStream())
            {
                zipStream.CopyTo(resultStream);
                return resultStream.ToArray();
            };
        }

        public class TransferBatch
        {

            public ReadOnlyCollection<System.Data.Common.DbColumn> Schema { get; set; }
            public string IdColumn {set; get;}
            public List<object[]> Values { get; set; }

        }
    }
}

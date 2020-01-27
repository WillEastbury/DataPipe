# DataPipe
Sample application to demonstrate shipping changes against rowversion high watermarks over a Service Bus Connection

##Requirements 

You will need at least one source Database of some flavour of SQL Database or SQL Server, preferably 2 to replicate between.
You will need to have implemented a soft delete in your tables, this will not replicate deletes.
Your database will also need to have a ROWVERSION (or timestamp) type column inside the schema of each table you want to replicate, and it will need to have a numeric, Identity as it's primary key.

You will need a Service bus with 2 connection strings, one for reading and one for writing.

## Setup the sample 

- You will need the following environment variables
-- sqlconn = Your SQL connection string (on either the consumer/source, or the writer/target)

-- sbconncon = Your SB connection string for the consumer/source (has writer access)
-- sbconnwri = Your SB connection string for the consumer/source (has reader access)

- Set the following List to the list of tables that you want to check and monitor for changes.

static List<string> SyncWhat = new List<string> { "SampleTable1", "SampleTable2", "SampleTable3", "SampleTable4" };


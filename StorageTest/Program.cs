using System;
using System.Diagnostics;
using System.Threading.Tasks;
using StorageTest.Entities;

namespace StorageTest
{
    static class Program
    {
        private const string CONNECTION_STRING = "ConnectionString goes here";

        static string GetUniqueValue()
        {
            return Guid.NewGuid().ToString("N");
        }

        static async Task Main(string[] args)
        {
            try
            {
                await RunTest();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw;
            }
        }

        static async Task RunTest()
        {
            // table name must start with letter
            var cosmosTableName = "a" + GetUniqueValue();
            var classicTableName = "a" + GetUniqueValue();

            #region Insert using COSMOS
            var cosmosTable = GetCosmosTable(cosmosTableName);

            var cosmosInsertStopwatch = new Stopwatch();
            cosmosInsertStopwatch.Start();
            for (int i = 0; i < 1000; i++)
            {
                var cosmosEntity = new CosmosEntity
                {
                    PartitionKey = GetUniqueValue(),
                    RowKey = GetUniqueValue(),
                    SomeData = GetUniqueValue(),
                    ETag = "*"
                };

                cosmosEntity.MoreData1 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData2 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData3 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData4 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData5 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData6 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData7 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData8 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData9 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData10 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData11 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData12 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData13 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData14 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData15 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData16 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData17 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData18 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData19 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData20 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData21 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData22 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData23 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData24 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData25 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData26 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData27 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData28 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData29 = Guid.NewGuid().ToString("N");
                cosmosEntity.MoreData30 = Guid.NewGuid().ToString("N");

                await cosmosTable.ExecuteAsync(Microsoft.Azure.Cosmos.Table.TableOperation.Insert(cosmosEntity)).ConfigureAwait(false);
            }
            cosmosInsertStopwatch.Stop();
            #endregion

            #region Insert using CLASSIC
            var classicTable = GetClassicTable(classicTableName);

            var classicInsertStopwatch = new Stopwatch();
            classicInsertStopwatch.Start();
            for (int i = 0; i < 1000; i++)
            {
                var classicEntity = new ClassicEntity
                {
                    PartitionKey = GetUniqueValue(),
                    RowKey = GetUniqueValue(),
                    SomeData = GetUniqueValue(),
                    ETag = "*"
                };

                classicEntity.MoreData1 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData2 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData3 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData4 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData5 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData6 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData7 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData8 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData9 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData10 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData11 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData12 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData13 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData14 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData15 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData16 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData17 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData18 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData19 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData20 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData21 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData22 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData23 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData24 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData25 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData26 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData27 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData28 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData29 = Guid.NewGuid().ToString("N");
                classicEntity.MoreData30 = Guid.NewGuid().ToString("N");

                await classicTable.ExecuteAsync(Microsoft.WindowsAzure.Storage.Table.TableOperation.Insert(classicEntity)).ConfigureAwait(false);
            }
            classicInsertStopwatch.Stop();
            #endregion

            #region Read CLASSIC entities using COSMOS
            var cosmosReadingClassicTable = GetCosmosTable(classicTableName);
            var cosmosReadingClassicStopwatch = new Stopwatch();
            cosmosReadingClassicStopwatch.Start();
            var corcl = await cosmosReadingClassicTable.ExecuteQuerySegmentedAsync(cosmosReadingClassicTable.CreateQuery<CosmosEntity>(), null).ConfigureAwait(false);
            cosmosReadingClassicStopwatch.Stop();
            Console.WriteLine($"Read {corcl.Results.Count} in {cosmosReadingClassicStopwatch.Elapsed} using COSMOS library");
            #endregion

            #region Read COSMOS entities using CLASSIC
            var classicReadingCosmosTable = GetClassicTable(cosmosTableName);
            var classicReadingCosmosStopwatch = new Stopwatch();
            classicReadingCosmosStopwatch.Start();
            var clrco = await classicReadingCosmosTable.ExecuteQuerySegmentedAsync(classicReadingCosmosTable.CreateQuery<ClassicEntity>(), null).ConfigureAwait(false);
            classicReadingCosmosStopwatch.Stop();
            Console.WriteLine($"Read {clrco.Results.Count} in {classicReadingCosmosStopwatch.Elapsed} using CLASSIC library");
            #endregion

            #region READ own entities using COSMOS
            var cosmosReadStopwatch = new Stopwatch();
            cosmosReadStopwatch.Start();
            var corco = await cosmosTable.ExecuteQuerySegmentedAsync(cosmosTable.CreateQuery<CosmosEntity>(), null).ConfigureAwait(false);
            cosmosReadStopwatch.Stop();
            Console.WriteLine($"Read {corco.Results.Count} in {cosmosReadStopwatch.Elapsed} using COSMOS library");
            #endregion

            #region READ own entities using CLASSIC
            var classicReadStopwatch = new Stopwatch();
            classicReadStopwatch.Start();
            var clrcl = await cosmosTable.ExecuteQuerySegmentedAsync(cosmosTable.CreateQuery<CosmosEntity>(), null).ConfigureAwait(false);
            classicReadStopwatch.Stop();
            Console.WriteLine($"Read {clrcl.Results.Count} in {classicReadStopwatch.Elapsed} using CLASSIC library");
            #endregion

            Console.WriteLine("Library\tInsert\t\t\tRead Self\t\tRead Other");
            Console.WriteLine($"Classic\t{classicInsertStopwatch.Elapsed}\t{classicReadStopwatch.Elapsed}\t{classicReadingCosmosStopwatch.Elapsed}");
            Console.WriteLine($"Cosmos\t{cosmosInsertStopwatch.Elapsed}\t{cosmosReadStopwatch.Elapsed}\t{cosmosReadingClassicStopwatch.Elapsed}");
        }

        static Microsoft.Azure.Cosmos.Table.CloudTable GetCosmosTable(string tableName)
        {
            var account = Microsoft.Azure.Cosmos.Table.CloudStorageAccount.Parse(CONNECTION_STRING);
            var client = Microsoft.Azure.Cosmos.Table.CloudStorageAccountExtensions.CreateCloudTableClient(account);
            var table = client.GetTableReference(tableName);

            table.CreateIfNotExistsAsync().Wait();

            return table;
        }

        static Microsoft.WindowsAzure.Storage.Table.CloudTable GetClassicTable(string tableName)
        {
            var account = Microsoft.WindowsAzure.Storage.CloudStorageAccount.Parse(CONNECTION_STRING);
            var client = account.CreateCloudTableClient();
            var table = client.GetTableReference(tableName);

            table.CreateIfNotExistsAsync().Wait();

            return table;
        }
    }
}
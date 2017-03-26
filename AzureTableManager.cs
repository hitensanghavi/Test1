using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace <Enter Your Namespace>
{
    public class AzureTableManager
    {

        private AzureTableManager()
        {

        }

        private CloudTableClient tableClient;
        private Dictionary<string, CloudTable> tables;

        public static AzureTableManager Create(string cnStr)
        {
            var manager = new AzureTableManager();

            // get cloud storage account
            var storageAccount = CloudStorageAccount.Parse(cnStr);

            // Create the table client.
            manager.tableClient = storageAccount.CreateCloudTableClient();

            return manager;
        }

        public async Task<CloudTable> GetTableAsync(string tblName, bool createIfNotExist = false)
        {
            if (tables == null)
                tables = new Dictionary<string, CloudTable>();

            if (tables.ContainsKey(tblName))
                return tables[tblName];

            // Get a reference to a table named "inviteRequests"
            CloudTable table = this.tableClient.GetTableReference(tblName);

            // Check if table exists
            bool exists = await table.ExistsAsync();

            // Create table if it does not exist
            if (!exists)
            {
                // Create the CloudTable if it does not exist
                if (!createIfNotExist)
                    return null;

                if (!await table.CreateIfNotExistsAsync())
                    return null;
            }

            tables.Add(tblName, table);

            return table;
        }

        public async Task<IList<TableResult>> BatchAsync<T>(string tblName, List<T> entities, AzureTableOperationEnum opType, bool createIfNotExist = false) where T : TableEntity
        {
            CloudTable table = await GetTableAsync(tblName, createIfNotExist);

            if (table == null)
                throw new Exception($"Table with name '{tblName}' not found");

            return await BatchAsync(table, entities, opType);
        }

        public async Task<IList<TableResult>> BatchAsync<T>(CloudTable table, List<T> entities, AzureTableOperationEnum opType) where T : TableEntity
        {

            // Create the batch operation.
            var op = new TableBatchOperation();

            // Create list to hold results
            var resultList = new List<TableResult>();

            // process 100 entities per batch 
            for (int i = 0; i < entities.Count; i++)
            {
                if (i != 0 && (i % 100 == 0 || i == entities.Count - 1))
                {
                    if (op != null)
                    {
                        // Execute the current batch insert operation.
                        var result = await table.ExecuteBatchAsync(op);

                        // Add the result list
                        resultList.AddRange(result);
                    }

                    op = new TableBatchOperation();
                }

                switch (opType)
                {
                    case AzureTableOperationEnum.Insert:
                        op.Insert(entities[i]);

                        break;
                    case AzureTableOperationEnum.Upsert:
                        op.InsertOrReplace(entities[i]);

                        break;
                    case AzureTableOperationEnum.Delete:
                        op.Delete(entities[i]);

                        break;
                    case AzureTableOperationEnum.Replace:
                        op.Replace(entities[i]);

                        break;
                    case AzureTableOperationEnum.Merge:
                        op.InsertOrMerge(entities[i]);

                        break;
                }
            }

            return resultList;
        }

        public async Task<TableResult> DeleteAsync<T>(CloudTable table, string pk, string rk) where T : TableEntity
        {
            // Create a retrieve operation that expects a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<T>(pk, rk);

            // Execute the operation.
            TableResult retrievedResult = await table.ExecuteAsync(retrieveOperation);

            // Assign the result to a CustomerEntity.
            T deleteEntity = (T)retrievedResult.Result;

            // Create the Delete TableOperation.
            if (deleteEntity != null)
            {
                TableOperation op = TableOperation.Delete(deleteEntity);

                // Execute the operation.
                return await table.ExecuteAsync(op);
            }

            return new TableResult() { HttpStatusCode = 404 };
        }

        public async Task<TableResult> InsertAsync<T>(CloudTable table, T entity) where T : TableEntity
        {
            // Create the TableOperation that inserts the customer entity.
            TableOperation insertOperation = TableOperation.Insert(entity);

            // Execute the insert operation.
            return await table.ExecuteAsync(insertOperation);
        }

        public async Task<TableResult> InsertOrReplaceAsync<T>(CloudTable table, T entity) where T : TableEntity
        {
            // Create the TableOperation that inserts the customer entity.
            TableOperation op = TableOperation.InsertOrReplace(entity);

            // Execute the insert operation.
            return await table.ExecuteAsync(op);
        }

        public async Task<TableResult> InsertOrMergeAsync<T>(CloudTable table, T entity) where T : TableEntity
        {
            // Create the TableOperation that inserts the customer entity.
            TableOperation op = TableOperation.InsertOrMerge(entity);

            // Execute the insert operation.
            return await table.ExecuteAsync(op);
        }

        public async Task<TableResult> ReplaceAsync<T>(CloudTable table, T localEntity, Action<T, T> updateCb) where T : TableEntity
        {
            // Retrieve entity
            TableOperation retrieveOperation = TableOperation.Retrieve<T>(localEntity.PartitionKey, localEntity.RowKey);

            // Execute the operation.
            TableResult retrievedResult = await table.ExecuteAsync(retrieveOperation);

            // Assign the result.
            T serverEntity = (T)retrievedResult.Result;

            if (serverEntity != null)
            {
                updateCb?.Invoke(localEntity, serverEntity); // update entity prior to 

                // Create the TableOperation that inserts the customer entity.
                TableOperation op = TableOperation.Replace(localEntity);

                // Execute the insert operation.
                return await table.ExecuteAsync(op);
            }
            else
            {
                return new TableResult() { HttpStatusCode = 404 };
            }
        }

        public async Task<TableResult> RetreiveAsync<T>(CloudTable table, string pk, string rk) where T : TableEntity
        {
            // Create a retrieve operation that takes a customer entity.
            TableOperation retrieveOperation = TableOperation.Retrieve<T>(pk, rk);

            // Execute the retrieve operation.
            return await table.ExecuteAsync(retrieveOperation);
        }

        public async Task<TableQuerySegment<T>> RetreiveSegmentAsync<T>(CloudTable table, string eventId, string token = null, int takeCount = 10, string rowKey = "", string op = QueryComparisons.Equal) where T : TableEntity, new()
        {
            var continuationToken = Utils.FromJson<TableContinuationToken>(token);

            // Initialize a default TableQuery to retrieve all the entities in the table.
            var tableQuery = new TableQuery<T>() { TakeCount = takeCount };

            var filterP = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, eventId);
            var filterR = TableQuery.GenerateFilterCondition("RowKey", op, rowKey);

            if (string.IsNullOrWhiteSpace(rowKey))
                tableQuery.Where(filterP);
            else
                tableQuery.Where(filterP + " and " + filterR);

            // Retrieve a segment.
            var tableQueryResult =
                await table.ExecuteQuerySegmentedAsync<T>(tableQuery, continuationToken);

            return tableQueryResult;
        }

        public async Task<TableQuerySegment<T>> RetreiveSegmentWithPrefixAsync<T>(CloudTable table, string eventId, string rowKeyFrom, string rowKeyTo, string token = null, int takeCount = 10) where T : TableEntity, new()
        {
            var continuationToken = Utils.FromJson<TableContinuationToken>(token);

            // Initialize a default TableQuery to retrieve all the entities in the table.
            var tableQuery = new TableQuery<T>() { TakeCount = takeCount };

            var filterP = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, eventId);
            var filterRFrom = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, rowKeyFrom);
            var filterRTo = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, rowKeyTo);

            tableQuery.Where(filterP + " and " + filterRFrom + " and " + filterRTo);

            // Retrieve a segment.
            var tableQueryResult =
                await table.ExecuteQuerySegmentedAsync<T>(tableQuery, continuationToken);

            return tableQueryResult;
        }
    }

    public class SampleTEntity : TableEntity
    {
        public SampleTEntity()
        {

        }

        public SampleTEntity(string userId, string fileName)
        {
            this.PartitionKey = userId;
            this.RowKey = fileName;
        }

        // Create other fields as auto implemented properties 
        public string OriginalUrl { get; set; }        
    }
}

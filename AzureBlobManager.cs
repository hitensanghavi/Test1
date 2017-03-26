using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure; // Namespace for CloudConfigurationManager
using Microsoft.WindowsAzure.Storage; // Namespace for CloudStorageAccount
using Microsoft.WindowsAzure.Storage.Blob; // Namespace for Blob storage types

namespace <Enter Namespace>
{
    public class AzureBlobManager
    {

        private AzureBlobManager()
        {

        }

        private CloudBlobClient blobClient;
        private Dictionary<string, CloudBlobContainer> containers;

        public static AzureBlobManager Create(string cnStr)
        {
            var manager = new AzureBlobManager();

            // Retrieve storage account from connection string.
            var storageAccount = CloudStorageAccount.Parse(cnStr);

            // Create the blob client.
            manager.blobClient = storageAccount.CreateCloudBlobClient();

            return manager;
        }

        public async Task<string> GetBlobAsString(object blobContainer, string name)
        {
            using (var st = await GetBlob(blobContainer, name))
            {
                StreamReader reader = new StreamReader(st);
                return reader.ReadToEnd();
            }
        }

        public async Task<Stream> GetBlob(object blobContainer, string name)
        {
            var container = await GetContainerAsync(blobContainer, false);

            if (container == null)
                throw new Exception($"Container not found");

            // Retrieve reference to blob
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(name);

            var memSt = new MemoryStream();
            await blockBlob.DownloadToStreamAsync(memSt);
            memSt.Position = 0;

            return memSt;
        }

        public async Task<CloudBlobContainer> GetContainerAsync(object blobContainer, bool createIfNotExist = false)
        {
            if (blobContainer.GetType() == typeof(CloudBlobContainer))
                return (CloudBlobContainer)blobContainer;

            if (blobContainer.GetType() != typeof(string))
                throw new ArgumentException("Parameter 'container' must be either a valid name of a blob container or CloubBlobContainer object");

            var containerName = (string)blobContainer;

            if (this.containers == null)
                this.containers = new Dictionary<string, CloudBlobContainer>();

            if (this.containers.ContainsKey(containerName))
                return this.containers[containerName];

            // Get a reference to a table named "inviteRequests"
            CloudBlobContainer container = this.blobClient.GetContainerReference(containerName);

            // Check if container exists
            bool exists = await container.ExistsAsync();

            // Create the CloudBlob container if it does not exist
            if (!exists)
            {
                if (!createIfNotExist)
                    return null;

                if (!await container.CreateIfNotExistsAsync())
                    return null;
            }

            this.containers.Add(containerName, container);

            //container.SetPermissions(new BlobContainerPermissions { PublicAccess = BlobContainerPublicAccessType.Blob });

            return container;
        }
        public async Task<CloudBlockBlob> UploadJsonAsync(object blobContainer, string blobName, string json, bool overwrite = false)
        {
            var blockBlob = await UploadTextAsync(blobContainer, blobName, json, overwrite);

            blockBlob.Properties.ContentType = "application/json";
            await blockBlob.SetPropertiesAsync();

            return blockBlob;

        }

        public async Task<CloudBlockBlob> UploadTextAsync(object blobContainer, string blobName, string text, bool overwrite = false)
        {
            using (var ms = new MemoryStream())
            {
                Utils.LoadStream(ms, text);
                return await UploadBlobAsync(blobContainer, blobName, ms, overwrite);
            }
        }

        public async Task<CloudBlockBlob> UploadBlobAsync(object blobContainer, string blobName, Stream stream, bool overwrite = false)
        {
            var container = await GetContainerAsync(blobContainer, false);

            if (container == null)
                throw new Exception($"Container not found");

            // Retrieve reference to a blob named "myblob".
            var blockBlob = container.GetBlockBlobReference(blobName);

            // Check if blob already exist
            var exists = await blockBlob.ExistsAsync();

            if (exists && !overwrite)
                return null;

            // Create or overwrite the "myblob" blob with contents from a local file.
            await blockBlob.UploadFromStreamAsync(stream);

            return blockBlob;
        }
    }
}

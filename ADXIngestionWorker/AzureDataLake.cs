using Azure.Core;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Mediation.Tool.KustoIngestor.Clients
{
    public class AzureDataLakeClientFactory
    {
        private readonly DataLakeFileSystemClient dataLakeFileSystemClient;
        private readonly string blobDirectoryPath;

        public AzureDataLakeClientFactory(string blobStorageAccount,
                                          string blobContainerName,
                                          string blobDirectoryPath,
                                          TokenCredential token)
        {
            Uri blobContainerUri = new Uri(string.Format("https://{0}.blob.core.windows.net/{1}",
                blobStorageAccount, blobContainerName));

            dataLakeFileSystemClient = new DataLakeFileSystemClient(blobContainerUri, token);
            this.blobDirectoryPath = blobDirectoryPath;
        }

        /// <summary>
        /// Returns an enumerator for files in the specified directory. If the directory does not exist, returns an empty enumerator.
        /// </summary>
        public async Task<IAsyncEnumerator<PathItem>> GetBlobEnumeratorAsync()
        {
            try
            {
                var directoryClient = dataLakeFileSystemClient.GetDirectoryClient(blobDirectoryPath);

                // Asynchronously check if directory exists
                bool directoryExists = await directoryClient.ExistsAsync();

                if (!directoryExists)
                {
                    Console.WriteLine($"Directory does not exist: {blobDirectoryPath}");
                    return AsyncEnumerable.Empty<PathItem>().GetAsyncEnumerator();
                }

                // Get the enumerator for files in the directory
                return dataLakeFileSystemClient.GetPathsAsync(blobDirectoryPath).GetAsyncEnumerator();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error while fetching enumerator for directory {blobDirectoryPath}: {ex.Message}");
                throw;  // Re-throwing exception so caller can handle it
            }
        }
    }
}

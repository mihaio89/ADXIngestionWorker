using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Azure.Storage.Sas;


namespace ADXIngestionWorker;

public class AzureDataLake
    {
        private DataLakeFileSystemClient _dataLakeFileSystemClient;
        private string _blobDirectoryPath;
        public AzureDataLake(string blobStorageAccount,
                                          string blobContainerName,
                                          string blobDirectoryPath,
                                          string blobAccountKeySecretName,
                                          SecretClient secretClient)
        {
            Uri blobContainerUri = new(string.Format("https://{0}.blob.core.windows.net/{1}",
                blobStorageAccount, blobContainerName));

            var blobAccountKey = secretClient.GetSecret(blobAccountKeySecretName);
            StorageSharedKeyCredential storageSharedKeyCredential = new(blobStorageAccount, blobAccountKey.Value.Value);

            _dataLakeFileSystemClient = new(blobContainerUri, storageSharedKeyCredential);

            _blobDirectoryPath = blobDirectoryPath;
        }

        public async Task<List<string>> ListFilesInDirectoryAsync()
        {
            List<string> blobSASUri = new List<string>();
            IAsyncEnumerator<PathItem> enumerator =
            _dataLakeFileSystemClient.GetPathsAsync(_blobDirectoryPath).GetAsyncEnumerator();

            await enumerator.MoveNextAsync();

            PathItem item = enumerator.Current;

            while (item != null && item.IsDirectory == false)
            {

                Uri SASKey = _dataLakeFileSystemClient.GetFileClient(item.Name).GenerateSasUri(DataLakeSasPermissions.All, DateTimeOffset.UtcNow.AddHours(1));
                blobSASUri.Add(SASKey.ToString());
                if (!await enumerator.MoveNextAsync())
                {
                    break;
                }

                item = enumerator.Current;
            }
            return blobSASUri;
        }
    }

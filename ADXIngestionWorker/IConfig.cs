namespace ADXIngestionWorker;

public interface IConfig
{
    //key vault tenant
    public string keyVaultPrefix { get; set; }
    public string tenantId { get; set; }
    public string clientId { get; set; }
    public string secretName { get; set; }


    //blob
    public string blobStorageAccount { get; set; }
    public string blobContainerName { get; set; }
    public string blobDirectoryPath { get; set; }
    public string blobAccountKeySecretName { get; set; }


    //kusto
    public string kustoMappingSchema { get; set; }
    public string kustoTable { get; set; }
    public string kustoDatabase { get; set; }
    public string kustoCluster { get; set; }
}

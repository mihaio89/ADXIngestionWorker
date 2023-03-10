using Microsoft.Extensions.Options;

namespace ADXIngestionWorker;
public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private IConfig _configuration;
    private DefaultAzureCredential _credential;
    //  private List<IngestionSet> IngestionSets; 

    private AzureDataLake _azureDataLake;
    private Kusto _kusto;


    public Worker(ILogger<Worker> logger, IOptions<Config> configuration, DefaultAzureCredential credential)
    {
        _logger = logger;
        _configuration = configuration.Value;
        _credential = credential;
    }

    public override Task StartAsync(CancellationToken stoppingToken)
    {

        //  foreach ( Config in _configuration.ConfigList) //if we have list of config

        var secretClient = new SecretClient(new Uri(string.Format("https://{0}.vault.azure.net", _configuration.keyVaultPrefix)), _credential);
        var aadAppSecret = secretClient.GetSecret(_configuration.secretName);
        var blobAccountKey = secretClient.GetSecret(_configuration.blobAccountKeySecretName);

        _azureDataLake = new(_configuration.blobStorageAccount, _configuration.blobContainerName, _configuration.blobDirectoryPath, _configuration.blobAccountKeySecretName, secretClient);
        _kusto = new(_configuration.clientId, _configuration.tenantId, _configuration.secretName, _configuration.kustoCluster, _configuration.kustoDatabase, _configuration.kustoTable, _configuration.kustoMappingSchema, secretClient);

        //  IngestionSets.Add(IngestionSet);

        return base.StartAsync(stoppingToken);

    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        List<string> directoryContent;
        while (!stoppingToken.IsCancellationRequested)
        {
            //  foreach (IngestionSet IngestionSet in IngestionSets)

            Console.WriteLine($"Scanning for File");
            directoryContent = await _azureDataLake.ListFilesInDirectoryAsync();

            foreach (string file in directoryContent)
            {
                await _kusto.ingestCMTelemetryAsync(file);
                Console.WriteLine(file);
            }
            directoryContent.Clear();

            await Task.Delay(30000, stoppingToken);
        }
    }
}
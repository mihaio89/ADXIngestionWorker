using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Mediation.Tool.KustoIngestor
{
    public class KustoIngestionSet
    {
        public AzureDataLakeClientFactory AzureDataLakeClientFactory { get; set; }
        public KustoClient KustoClient { get; set; }
        public BlobContainerClient BlobContainerClient { get; set; }
    }

    public class Worker : BackgroundService
    {
        private readonly IConfiguration configuration;
        private readonly DefaultAzureCredential credential;
        private readonly List<KustoIngestionSet> kustoIngestionSets;
        private readonly IMemoryCache memoryCache;
        private readonly ILogger<Worker> logger;
        private readonly bool isDevelopment;

        private const int MaxFilesPerRun = 1000;
        private const int FileRollOverDelaySeconds = 61;
        private const int DefaultCacheExpiryMinutes = 3;

        public KustoIngestorWorker(
            ILogger<KustoIngestorWorker> logger,
            IConfiguration configuration,
            DefaultAzureCredential credential,
            IMemoryCache memoryCache)
        {
            this.logger = logger;
            this.configuration = configuration;
            this.credential = credential;
            this.memoryCache = memoryCache;
            this.isDevelopment = this.configuration["DOTNET_ENVIRONMENT"] == "Development";
            this.kustoIngestionSets = new List<KustoIngestionSet>();
        }

        public override Task StartAsync(CancellationToken stoppingToken)
        {
            var kustoIngestorDetails = this.configuration
                .GetSection("KustoIngestorConfig")
                .GetSection("kustoIngestorDetails")
                .Get<KustoIngestorDetail[]>();

            foreach (KustoIngestorDetail kid in kustoIngestorDetails)
            {
                var kustoIngestionSet = new KustoIngestionSet
                {
                    AzureDataLakeClientFactory = new AzureDataLakeClientFactory(kid.blobStorageAccount, kid.blobContainerName, kid.blobDirectoryPath, this.credential),
                    KustoClient = new KustoClient(kid, this.isDevelopment, this.configuration["usermi"]),
                    BlobContainerClient = new BlobServiceClient(new Uri($"https://{kid.blobStorageAccount}.blob.core.windows.net"), this.credential)
                        .GetBlobContainerClient(kid.blobContainerName)
                };

                this.kustoIngestionSets.Add(kustoIngestionSet);
            }

            return base.StartAsync(stoppingToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                foreach (var kustoIngestionSet in this.kustoIngestionSets)
                {
                    var fileSystem = kustoIngestionSet.AzureDataLakeClientFactory._dataLakeFileSystemClient;
                    var blobEnumerator = kustoIngestionSet.AzureDataLakeClientFactory.getBlobEnumerator();

                    if (blobEnumerator == null) continue;

                    int counter = 0;

                    try
                    {
                        await blobEnumerator.MoveNextAsync();
                        var item = blobEnumerator.Current;

                        while (item != null && !item.IsDirectory)
                        {
                            DateTimeOffset thresholdTime = DateTime.UtcNow.AddSeconds(-FileRollOverDelaySeconds);
                            
                            if (this.memoryCache.TryGetValue(item.Name, out _))
                            {
                                this.logger.LogInformation("Skipping {FileName}: Already processed.", item.Name);
                            }
                            else if (item.CreatedOn > thresholdTime)
                            {
                                this.logger.LogInformation("Skipping {FileName}: Not rolled over yet. Created: {CreatedTime}, Threshold: {ThresholdTime}", 
                                    item.Name, item.CreatedOn, thresholdTime);
                            }
                            else
                            {
                                this.logger.LogInformation("Ingesting {FileName}", item.Name);

                                int cacheExpiryMinutes = GetCacheExpiryMinutes();
                                this.memoryCache.Set(item.Name, 0, TimeSpan.FromMinutes(cacheExpiryMinutes));

                                try
                                {
                                    var blobClient = kustoIngestionSet.BlobContainerClient.GetBlobClient(item.Name);
                                    
                                    using var downloadStream = await blobClient.OpenReadAsync();
                                    await kustoIngestionSet.KustoClient.ingest(downloadStream);
                                    
                                    // Delete the blob after successful ingestion
                                    await blobClient.DeleteAsync();
                                }
                                catch (Exception ex)
                                {
                                    this.logger.LogError(ex, "Error during ingestion {FileName}", item.Name);
                                }

                                counter++;
                            }

                            if (!await blobEnumerator.MoveNextAsync() || counter >= MaxFilesPerRun)
                            {
                                break;
                            }

                            item = blobEnumerator.Current;
                        }
                    }
                    catch (Exception ex)
                    {
                        this.logger.LogError(ex, "Error occurred during blob enumeration.");
                    }
                }

                await Task.Delay(500, stoppingToken);
            }
        }

        private int GetCacheExpiryMinutes()
        {
            string cacheExpiryMinutesStr = Environment.GetEnvironmentVariable("CACHE_EXPIRY_MINUTES");
            return int.TryParse(cacheExpiryMinutesStr, out int cacheExpiryMinutes) 
                ? cacheExpiryMinutes 
                : DefaultCacheExpiryMinutes;
        }
    }
}

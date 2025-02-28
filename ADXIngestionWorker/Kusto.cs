using Azure.Core;
using Azure.Identity;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Ingestion;
using Kusto.Ingest;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Threading.Tasks;


    public class KustoClient
    {
        private readonly KustoConnectionStringBuilder kscb;
        private readonly KustoIngestionProperties kip;
        private readonly IKustoIngestClient kic;
        private readonly ILogger<KustoClient> _logger;
        private readonly DefaultAzureCredential _credential;

        // Constructor with Dependency Injection for logging and DefaultAzureCredential
        public KustoClient(KustoIngestorDetail kid,
                           bool isDevelopment,
                           string managedIdentity,
                           ILogger<KustoClient> logger,
                           DefaultAzureCredential credential)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _credential = credential ?? throw new ArgumentNullException(nameof(credential));

            kscb = isDevelopment 
                ? new KustoConnectionStringBuilder(kid.kustoClusterIngestion)
                    .WithAadUserTokenAuthentication(GetToken(kid.kustoCluster)) 
                : new KustoConnectionStringBuilder(kid.kustoClusterIngestion)
                    .WithAadUserManagedIdentity(managedIdentity);

            kic = KustoIngestFactory.CreateQueuedIngestClient(kscb);

            kip = new KustoIngestionProperties(databaseName: kid.kustoDatabase, tableName: kid.kustoTable)
            {
                IngestionMapping = new IngestionMapping()
                {
                    IngestionMappingKind = IngestionMappingKind.Json,
                    IngestionMappingReference = kid.kustoMappingSchema,
                },
                Format = DataSourceFormat.json,
            };
        }

        public async Task IngestCMTelemetryAsync(Stream stream)
        {
            if (stream == null)
            {
                _logger.LogError("Stream is null, ingestion cannot proceed.");
                return;
            }

            try
            {
                // Ensure stream is properly disposed of after usage
                using (stream)
                {
                    await kic.IngestFromStreamAsync(stream, kip);
                }
                _logger.LogInformation("Ingestion successful.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred during ingestion.");
            }
        }

        private async Task<string> GetToken(string clusterUri)
        {
            var requestContext = new TokenRequestContext(new[] { clusterUri });

            try
            {
                var token = await _credential.GetTokenAsync(requestContext);
                return token.Token;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while retrieving the token.");
                throw new InvalidOperationException("Failed to get authentication token.", ex);
            }
        }
    }
}

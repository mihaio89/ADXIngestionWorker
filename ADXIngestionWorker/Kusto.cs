using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Ingestion;
using Kusto.Ingest;

namespace ADXIngestionWorker;
    public class Kusto
    {
        private KustoConnectionStringBuilder _ksc;
        private KustoIngestionProperties _kp;
        private IKustoIngestClient _kc;

        public Kusto(string applicationClientID,
                                      string applicationKey,
                                      string authority,
                                      string clusterUrl,
                                      string databaseName,
                                      string tableName,
                                      string mappingName,
                                      SecretClient secretClient)
        {
            var tenantSecret = secretClient.GetSecret(authority);

            _ksc = new KustoConnectionStringBuilder(clusterUrl)
                  .WithAadApplicationKeyAuthentication(
                     applicationClientId: applicationClientID,
                     applicationKey: tenantSecret.Value.Value,
                     authority: applicationKey);

            _kc = KustoIngestFactory.CreateQueuedIngestClient(_ksc);

            _kp = new KustoIngestionProperties(databaseName: databaseName, tableName: tableName)
            {
                IngestionMapping = new IngestionMapping()
                {
                    IngestionMappingKind = IngestionMappingKind.Json,
                    IngestionMappingReference = mappingName,
                },
                Format = DataSourceFormat.json,
            };
        }
        public async Task ingestCMTelemetryAsync(string SASKey)
        {
            try
            {
                await _kc.IngestFromStorageAsync(uri: SASKey, ingestionProperties: _kp, new StorageSourceOptions() { DeleteSourceOnSuccess = true });
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }
    }

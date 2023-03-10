using ADXIngestionWorker;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        var configuration = getConfiguration();
        services.Configure<Config>(configuration.GetSection(nameof(Config)));
        services.AddSingleton(cred =>
        {
            return new DefaultAzureCredential(false);
        });
        services.AddHostedService<Worker>();
    })
    .Build();
IConfigurationRoot getConfiguration()
{
    var environment = Environment.GetEnvironmentVariable("Env") == null ? "DEV" : Environment.GetEnvironmentVariable("Env")?.ToUpperInvariant();
    string settings;

    switch (environment)
    {
        case "DEV":
            settings = "appsettings.Development.json";
            break;

        default:
            throw new ArgumentException($"Unsupported value for environment variable 'Environment': {environment}");
    }


    var config = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json")
        .AddJsonFile(settings)
        .AddEnvironmentVariables()
        .Build();

    return config;
}
await host.RunAsync();


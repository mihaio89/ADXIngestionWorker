using ADXIngestionWorker;

using Azure.Identity;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

public class Program
{
    public static void Main(string[] args)
    {
        var builder = Host.CreateDefaultBuilder(args)
            .ConfigureAppConfiguration(ConfigureAppSettings)
            .ConfigureServices(ConfigureServices)
            .Build();

        builder.Run();
    }

    private static void ConfigureAppSettings(HostBuilderContext context, IConfigurationBuilder configBuilder)
    {
        configBuilder.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                     .AddJsonFile($"appsettings.{context.HostingEnvironment.EnvironmentName}.json", optional: true, reloadOnChange: true)
                     .AddEnvironmentVariables()
                     .AddCommandLine(Environment.GetCommandLineArgs());
    }

    private static void ConfigureServices(HostBuilderContext hostingContext, IServiceCollection services)
    {
        services.AddSingleton(provider => CreateAzureCredential(hostingContext.Configuration));
        services.AddMemoryCache();
        services.AddHostedService<KustoIngestorWorker>();
    }

    private static DefaultAzureCredential CreateAzureCredential(IConfiguration configuration)
    {
        var options = new DefaultAzureCredentialOptions
        {
            ManagedIdentityClientId = configuration["UserAssignedMIClientID"],
            ExcludeAzureCliCredential = true,
            ExcludeEnvironmentCredential = true,
            ExcludeSharedTokenCacheCredential = true,
            ExcludeInteractiveBrowserCredential = true
        };

        return new DefaultAzureCredential(options);
    }
}

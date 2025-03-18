using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Energinet.DataHub.Measurements.Client.Tests.Fixtures;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddInMemoryConfiguration(
        this IServiceCollection services,
        Dictionary<string, string?> configurations)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configurations)
            .Build();

        services.AddScoped<IConfiguration>(_ => configuration);

        return services;
    }
}

using Energinet.DataHub.Measurements.Client.Authentication;
using Energinet.DataHub.Measurements.Client.Extensions.Options;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;

/// <summary>
/// Extension methods for adding Measurement Client dependencies to applications.
/// </summary>
public static class ClientExtensions
{
    /// <summary>
    /// Register Measurement Client for use in application.
    /// </summary>
    public static IServiceCollection AddMeasurementsClient(this IServiceCollection services)
    {
        services
            .AddOptions<MeasurementHttpClientOptions>()
            .BindConfiguration(MeasurementHttpClientOptions.SectionName)
            .ValidateDataAnnotations();

        services.AddHttpClient(MeasurementsHttpClientNames.MeasurementsApi, (serviceProvider, httpClient) =>
        {
            var options = serviceProvider.GetRequiredService<IOptions<MeasurementHttpClientOptions>>().Value;
            httpClient.BaseAddress = new Uri(options.BaseAddress);
        });

        services.AddHttpContextAccessor();
        services.AddAuthorizedHttpClient();

        services.AddScoped<IMeasurementsClient, MeasurementsClient>();

        return services;
    }

    private static IServiceCollection AddAuthorizedHttpClient(this IServiceCollection serviceCollection)
    {
        return serviceCollection
            .AddSingleton<IAuthorizedHttpClientFactory>(provider => new AuthorizedHttpClientFactory(
                provider.GetRequiredService<IHttpClientFactory>(),
                () =>
                {
                    var httpContextAccessor = provider.GetRequiredService<IHttpContextAccessor>();
                    return (string?)httpContextAccessor.HttpContext?.Request.Headers.Authorization ?? string.Empty;
                }));
    }
}

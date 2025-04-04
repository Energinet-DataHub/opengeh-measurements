using System.Net.Http.Headers;
using Azure.Core;
using Azure.Identity;
using Energinet.DataHub.Measurements.Client.Extensions.Options;
using Energinet.DataHub.Measurements.Client.Factories;
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
            var measurementHttpClientOptions = serviceProvider.GetRequiredService<IOptions<MeasurementHttpClientOptions>>().Value;
            httpClient.BaseAddress = new Uri(measurementHttpClientOptions.BaseAddress);

            var token = new DefaultAzureCredential()
                .GetToken(new TokenRequestContext([measurementHttpClientOptions.ApplicationIdUri]), CancellationToken.None)
                .Token;

            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
        });

        services.AddScoped<MeasurementAggregationFactory>();
        services.AddScoped<IMeasurementsClient, MeasurementsClient>();

        return services;
    }
}

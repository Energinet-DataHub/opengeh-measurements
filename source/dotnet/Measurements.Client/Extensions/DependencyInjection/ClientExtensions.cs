using Azure.Identity;
using Energinet.DataHub.Measurements.Client.Authentication;
using Energinet.DataHub.Measurements.Client.Extensions.Options;
using Energinet.DataHub.Measurements.Client.ResponseParsers;
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

        services.AddSingleton<IAuthorizationHeaderProvider>(serviceProvider =>
        {
            var credential = new DefaultAzureCredential();
            var options = serviceProvider.GetRequiredService<IOptions<MeasurementHttpClientOptions>>().Value;

            return new AuthorizationHeaderProvider(credential, options.ApplicationIdUri);
        });

        services.AddHttpClient(MeasurementsHttpClientNames.MeasurementsApi, (serviceProvider, httpClient) =>
        {
            var measurementHttpClientOptions = serviceProvider.GetRequiredService<IOptions<MeasurementHttpClientOptions>>().Value;
            var authorizationHeaderProvider = serviceProvider.GetRequiredService<IAuthorizationHeaderProvider>();
            var authorizationHeader = authorizationHeaderProvider.CreateAuthorizationHeader();

            httpClient.BaseAddress = new Uri(measurementHttpClientOptions.BaseAddress);
            httpClient.DefaultRequestHeaders.Authorization = authorizationHeader;
        });

        services.AddScoped<IMeasurementsForDayResponseParser, MeasurementsForDayResponseParser>();
        services.AddScoped<IMeasurementsClient, MeasurementsClient>();

        return services;
    }
}

using System.Net.Http.Headers;
using Energinet.DataHub.Core.App.Common.Extensions.DependencyInjection;
using Energinet.DataHub.Core.App.Common.Identity;
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
    /// Register Measurement Client for use in the application.
    /// </summary>
    public static IServiceCollection AddMeasurementsClient(
        this IServiceCollection services,
        CustomAuthorizationHandler? customAuthorizationHandler = null)
    {
        services
            .AddOptions<MeasurementHttpClientOptions>()
            .BindConfiguration(MeasurementHttpClientOptions.SectionName)
            .ValidateDataAnnotations();

        if (customAuthorizationHandler != null)
        {
            services
                .AddHttpClient(MeasurementsHttpClientNames.MeasurementsApi, (serviceProvider, httpClient) =>
                {
                    var measurementHttpClientOptions = serviceProvider.GetRequiredService<IOptions<MeasurementHttpClientOptions>>().Value;
                    httpClient.BaseAddress = new Uri(measurementHttpClientOptions.BaseAddress);
                })
                .AddHttpMessageHandler(() => customAuthorizationHandler);
        }
        else
        {
            services.AddTokenCredentialProvider();
            services.AddAuthorizationHeaderProvider();

            services.AddHttpClient(MeasurementsHttpClientNames.MeasurementsApi, (serviceProvider, httpClient) =>
            {
                var measurementHttpClientOptions = serviceProvider.GetRequiredService<IOptions<MeasurementHttpClientOptions>>().Value;
                var authorizationHeaderProvider = serviceProvider.GetRequiredService<IAuthorizationHeaderProvider>();
                var authorizationHeader = authorizationHeaderProvider.CreateAuthorizationHeader(measurementHttpClientOptions.ApplicationIdUri);

                httpClient.BaseAddress = new Uri(measurementHttpClientOptions.BaseAddress);
                httpClient.DefaultRequestHeaders.Authorization = authorizationHeader;
            });
        }

        services.AddScoped<IMeasurementsForDateResponseParser, MeasurementsForDateResponseParser>();
        services.AddScoped<IMeasurementsClient, MeasurementsClient>();

        return services;
    }
}

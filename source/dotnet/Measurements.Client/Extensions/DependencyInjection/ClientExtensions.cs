using System.Net.Http.Headers;
using Azure.Core;
using Azure.Identity;
using Energinet.DataHub.Measurements.Client.Authentication;
using Energinet.DataHub.Measurements.Client.Extensions.Options;
using Energinet.DataHub.Measurements.Client.ResponseParsers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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
        Action<MeasurementClientOptions>? configureOptions = null)
    {
        // Configure options
        var options = new MeasurementClientOptions();
        configureOptions?.Invoke(options);

        services
            .AddOptions<MeasurementHttpClientOptions>()
            .BindConfiguration(MeasurementHttpClientOptions.SectionName)
            .ValidateDataAnnotations();

        // If no authorization header is provided, one for Entra is used
        if (options.AuthorizationHeaderProvider != null)
        {
            services.AddSingleton(options.AuthorizationHeaderProvider);
        }
        else
        {
            services.AddSingleton<IAuthorizationHeaderProvider>(serviceProvider =>
            {
                var measurementsHttpOptions = serviceProvider.GetRequiredService<IOptions<MeasurementHttpClientOptions>>().Value;
                return new AuthorizationHeaderProvider(new DefaultAzureCredential(), measurementsHttpOptions.ApplicationIdUri);
            });
        }

        services.AddHttpClient(MeasurementsHttpClientNames.MeasurementsApi, (serviceProvider, httpClient) =>
        {
            var measurementHttpClientOptions = serviceProvider.GetRequiredService<IOptions<MeasurementHttpClientOptions>>().Value;
            var authorizationHeaderProvider = serviceProvider.GetRequiredService<IAuthorizationHeaderProvider>();
            var authorizationHeader = authorizationHeaderProvider.CreateAuthorizationHeaderValue();

            httpClient.BaseAddress = new Uri(measurementHttpClientOptions.BaseAddress);
            httpClient.DefaultRequestHeaders.Authorization = authorizationHeader;
        });

        services.AddScoped<IMeasurementsForDateResponseParser, MeasurementsForDateResponseParser>();
        services.AddScoped<IMeasurementsClient, MeasurementsClient>();

        return services;
    }
}

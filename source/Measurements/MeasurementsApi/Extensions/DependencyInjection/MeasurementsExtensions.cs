using Energinet.DataHub.Core.App.Common.Extensions.DependencyInjection;

namespace MeasurementsApi.Extensions.DependencyInjection;

public static class MeasurementsExtensions
{
    public static IServiceCollection AddMeasurementsModule(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddNodaTimeForApplication();

        return services;
    }
}

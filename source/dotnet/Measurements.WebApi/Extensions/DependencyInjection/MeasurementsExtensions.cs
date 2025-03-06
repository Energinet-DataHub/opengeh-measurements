using Energinet.DataHub.Core.App.Common.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Infrastructure.Handlers;

namespace Energinet.DataHub.Measurements.WebApi.Extensions.DependencyInjection;

public static class MeasurementsExtensions
{
    public static IServiceCollection AddMeasurementsModule(this IServiceCollection services)
    {
        services.AddNodaTimeForApplication();
        services.AddScoped<IMeasurementsHandler, MeasurementsHandler>();

        return services;
    }
}

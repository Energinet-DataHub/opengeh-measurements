using Energinet.DataHub.Core.App.Common.Extensions.DependencyInjection;
using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Infrastructure.Handlers;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;

namespace Energinet.DataHub.Measurements.WebApi.Extensions.DependencyInjection;

public static class MeasurementsExtensions
{
    public static IServiceCollection AddMeasurementsModule(this IServiceCollection services, IConfiguration configuration)
    {
        services
            .AddOptions<DatabricksSchemaOptions>()
            .BindConfiguration(DatabricksSchemaOptions.SectionName)
            .ValidateDataAnnotations();

        services.AddDatabricksSqlStatementExecution(configuration.GetSection(DatabricksSqlStatementOptions.DatabricksOptions));

        services.AddNodaTimeForApplication();
        services.AddScoped<IMeasurementsHandler, MeasurementsHandler>();
        services.AddScoped<IMeasurementsRepository, MeasurementsRepository>();
        services.AddScoped<IJsonSerializer, JsonSerializer>();

        return services;
    }
}

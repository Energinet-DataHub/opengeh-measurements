using System.Reflection;
using Asp.Versioning;
using Energinet.DataHub.Core.App.WebApp.Extensions.DependencyInjection;

namespace Energinet.DataHub.Measurements.WebApi.Extensions.DependencyInjection;

public static class SwaggerExtensions
{
    public static IServiceCollection AddSwagger(this IServiceCollection services)
    {
        return services
            .AddSwaggerForWebApp(
                executingAssembly: Assembly.GetExecutingAssembly(),
                swaggerUITitle: "DataHub 3 Measurements API")
            .AddApiVersioningForWebApp(new ApiVersion(1.0))
            .AddEndpointsApiExplorer();
    }
}

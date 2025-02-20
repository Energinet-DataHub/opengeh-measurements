using System.Reflection;
using Asp.Versioning;
using Energinet.DataHub.Core.App.WebApp.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.MeasurementsApi.Extensions.DependencyInjection;

namespace Energinet.DataHub.Measurements.MeasurementsApi;

public static class ApplicationFactory
{
    public static WebApplication CreateApplication(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        // Common
        builder.Services.AddApplicationInsightsForWebApp(subsystemName: "Measurements");
        builder.Services.AddHealthChecksForWebApp();

        // Modules
        builder.Services.AddMeasurementsModule();

        // Http channels
        builder.Services.AddControllers();

        // => Open API generation
        builder.Services.AddSwaggerForWebApp(Assembly.GetExecutingAssembly(), swaggerUITitle: "Measurements API");

        // => API versioning
        builder.Services.AddApiVersioningForWebApp(defaultVersion: new ApiVersion(1, 0));

        // => Authentication/authorization
        builder.Services
            .AddAuthenticationForWebApp(builder.Configuration)
            .AddPermissionAuthorizationForWebApp();

        return builder.Build();
    }
}

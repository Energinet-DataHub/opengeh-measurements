using System.Reflection;
using Asp.Versioning;
using Energinet.DataHub.Core.App.WebApp.Extensions.DependencyInjection;
using MeasurementsApi.Extensions.DependencyInjection;

namespace MeasurementsApi;

public class ApplicationFactory
{
    public static WebApplication CreateApplication(string[] args, Dictionary<string, string?>? applicationSettings = default)
    {
        var builder = WebApplication.CreateBuilder(args);

        // Common
        builder.Services.AddApplicationInsightsForWebApp(subsystemName: "Measurements");
        builder.Services.AddHealthChecksForWebApp();

        // Modules
        builder.Services.AddMeasurementsModule(builder.Configuration);

        // Http channels
        builder.Services.AddControllers();

        // => Open API generation
        builder.Services.AddSwaggerForWebApp(Assembly.GetExecutingAssembly(), swaggerUITitle: "Measurements API");

        // => API versioning
        builder.Services.AddApiVersioningForWebApp(defaultVersion: new ApiVersion(1, 0));

        // => Authentication/authorization
        builder.Services
            .AddJwtBearerAuthenticationForWebApp(builder.Configuration)
            .AddPermissionAuthorizationForWebApp();

        return builder.Build();
    }
}

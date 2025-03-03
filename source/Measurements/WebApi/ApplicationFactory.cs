using Energinet.DataHub.Core.App.WebApp.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.WebApi.Extensions.DependencyInjection;

namespace Energinet.DataHub.Measurements.WebApi;

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
        builder.Services.AddSwagger();

        // => Authentication/authorization
        builder.Services
            .AddAuthenticationForWebApp(builder.Configuration)
            .AddPermissionAuthorizationForWebApp();

        return builder.Build();
    }
}

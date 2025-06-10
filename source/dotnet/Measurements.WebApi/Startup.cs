using Energinet.DataHub.Core.App.WebApp.Extensions.Builder;
using Energinet.DataHub.Core.App.WebApp.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.WebApi.Extensions.DependencyInjection;
using NodaTime;
using NodaTime.Serialization.SystemTextJson;

namespace Energinet.DataHub.Measurements.WebApi;

public class Startup(IConfiguration configuration)
{
    public void ConfigureServices(IServiceCollection services)
    {
        // Common
        services.AddApplicationInsightsForWebApp(subsystemName: "Measurements");
        services.AddHealthChecksForWebApp();

        // Modules
        services.AddMeasurementsModule(configuration);

        // Http channels
        services
            .AddControllers()
            .AddJsonOptions(options =>
            {
                options.JsonSerializerOptions.ConfigureForNodaTime(DateTimeZoneProviders.Tzdb);
            });

        // => Open API generation
        services.AddSwagger();

        // => Authentication/authorization
        // TODO: When support for B2C authentication is removed, the 'AddAuthenticationForWebApp' must be replaced
        // with 'AddSubsystemAuthenticationForWebApp' from Energinet.DataHub.Core.App.Common.Extensions.DependencyInjection
        services
            .AddAuthenticationForWebApp(configuration)
            .AddAuthorizationForWebApp();
    }

    public void Configure(IApplicationBuilder app)
    {
        app.UseRouting();
        app.UseSwaggerForWebApp();
        app.UseHttpsRedirection();

        app.UseAuthentication();
        app.UseAuthorization();

        app.UseEndpoints(endpoints =>
        {
            endpoints.MapControllers();

            endpoints.MapLiveHealthChecks();
            endpoints.MapReadyHealthChecks();
            endpoints.MapStatusHealthChecks();
        });
    }
}

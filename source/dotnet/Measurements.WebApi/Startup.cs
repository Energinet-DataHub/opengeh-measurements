using Asp.Versioning;
using Asp.Versioning.Conventions;
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
        services
            .AddAuthenticationForWebApp(configuration)
            .AddAuthorizationForWebApp();

        // Versioning
        services.AddApiVersioning(
            options =>
            {
                options.DefaultApiVersion = new ApiVersion(1.0);
                options.AssumeDefaultVersionWhenUnspecified = true;
                options.ReportApiVersions = true;
                options.ApiVersionReader = new UrlSegmentApiVersionReader();
            })
            .AddMvc()
            .AddApiExplorer(
                options =>
                {
                    options.GroupNameFormat = "'v'VVV";
                    options.SubstituteApiVersionInUrl = true;
                });
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
            // Define an ApiVersionSet containing all API versions
            var versionSet = endpoints.NewApiVersionSet()
                .HasApiVersion(1.0)
                .ReportApiVersions()
                .Build();

            endpoints
                .MapGroup("v{version:apiVersion}/measurements")
                .WithApiVersionSet(versionSet)
                .HasApiVersion(1.0)
                .MapControllers();

            endpoints
                .MapGroup("measurements")
                .WithApiVersionSet(versionSet)
                .HasApiVersion(1.0)
                .WithTags("Default v1")
                .MapControllers();

            endpoints.MapLiveHealthChecks();
            endpoints.MapReadyHealthChecks();
            endpoints.MapStatusHealthChecks();
        });
    }
}

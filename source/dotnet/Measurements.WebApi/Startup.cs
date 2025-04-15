using Asp.Versioning;
using Asp.Versioning.Builder;
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
                options.DefaultApiVersion = new ApiVersion(1);
                options.AssumeDefaultVersionWhenUnspecified = true;
                options.ReportApiVersions = true;
                options.ApiVersionReader = new UrlSegmentApiVersionReader();
            })
            .AddMvc()
            .AddApiExplorer(
                options =>
                {
                    options.GroupNameFormat = "'v'V";
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
            // Define an ApiVersionSet
            var versionSet = endpoints.NewApiVersionSet()
                .HasApiVersion(1)
                .HasApiVersion(2)
                .ReportApiVersions() // Optional: Include supported versions in the response headers
                .Build();

            endpoints
                .MapGroup("v{version:apiVersion}/measurements")
                .WithApiVersionSet(versionSet)
                .HasApiVersion(1)
                .HasApiVersion(2)
                .WithTags("v1")
                .MapControllers();

            endpoints
                .MapGroup("measurements")
                .WithApiVersionSet(versionSet)
                .HasApiVersion(1)
                .WithTags("Default v1")
                .MapControllers();

            // endpoints.MapGet("v{version:apiVersion}/measurements", () => "Version 1 - Get all measurements");
            // endpoints.MapControllers().WithApiVersionSet(versionSet);
            endpoints.MapLiveHealthChecks();
            endpoints.MapReadyHealthChecks();
            endpoints.MapStatusHealthChecks();
            //endpoints.MapControllers().WithApiVersionSet(versionSet);
        });

        // ConfigureVersioning(app);
    }
}

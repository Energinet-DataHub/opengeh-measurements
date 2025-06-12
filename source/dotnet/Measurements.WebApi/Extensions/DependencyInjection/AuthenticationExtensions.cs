using Energinet.DataHub.Core.App.WebApp.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.WebApi.Constants;
using Microsoft.AspNetCore.Authentication.JwtBearer;

namespace Energinet.DataHub.Measurements.WebApi.Extensions.DependencyInjection;

public static class AuthenticationExtensions
{
    public static IServiceCollection AddAuthenticationForWebApp(this IServiceCollection services, IConfiguration configuration)
    {
        var isGeneratorToolBuild = Environment.GetEnvironmentVariable("GENERATOR_TOOL_BUILD") == "Yes";
        if (isGeneratorToolBuild) return services;

        services
            .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer(AuthenticationSchemes.B2C, options =>
            {
                var b2CAuthenticationOptions = configuration
                    .GetSection(B2CAuthenticationOptions.SectionName)
                    .Get<B2CAuthenticationOptions>();
                var authority = $"https://login.microsoftonline.com/{b2CAuthenticationOptions?.TenantId}/v2.0";

                options.Audience = b2CAuthenticationOptions?.ResourceId;
                options.Authority = authority;
            });

        services.AddSubsystemAuthenticationForWebApp(configuration);

        return services;
    }
}

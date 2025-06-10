using Energinet.DataHub.Core.App.Common.Extensions.Options;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.WebApi.Constants;
using Microsoft.AspNetCore.Authentication.JwtBearer;

namespace Energinet.DataHub.Measurements.WebApi.Extensions.DependencyInjection;

public static class AuthenticationExtensions
{
    public static IServiceCollection AddAuthenticationForWebApp(this IServiceCollection services, IConfiguration configuration)
    {
        services
            .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer(AuthenticationSchemes.Default, options =>
            {
                var authenticationOptions = configuration
                    .GetRequiredSection(SubsystemAuthenticationOptions.SectionName)
                    .Get<SubsystemAuthenticationOptions>();

                options.Audience = authenticationOptions?.ApplicationIdUri;
                options.Authority = authenticationOptions?.Issuer;
            })
            .AddJwtBearer(AuthenticationSchemes.B2C, options =>
            {
                var b2CAuthenticationOptions = configuration
                    .GetSection(B2CAuthenticationOptions.SectionName)
                    .Get<B2CAuthenticationOptions>();
                var authority = $"https://login.microsoftonline.com/{b2CAuthenticationOptions?.TenantId}/v2.0";

                options.Audience = b2CAuthenticationOptions?.ResourceId;
                options.Authority = authority;
            });

        return services;
    }
}

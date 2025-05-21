using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;

namespace Energinet.DataHub.Measurements.WebApi.Extensions.DependencyInjection;

public static class AuthenticationExtensions
{
    public static IServiceCollection AddAuthenticationForWebApp(this IServiceCollection services, IConfiguration configuration)
    {
        var entraAuthenticationOptions = configuration
            .GetSection(EntraAuthenticationOptions.SectionName)
            .Get<EntraAuthenticationOptions>();
        var b2CAuthenticationOptions = configuration
            .GetSection(B2CAuthenticationOptions.SectionName)
            .Get<B2CAuthenticationOptions>();
        var authority = $"https://login.microsoftonline.com/{b2CAuthenticationOptions?.TenantId}/v2.0";

        services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
            .AddJwtBearer(options =>
            {
                options.Authority = entraAuthenticationOptions?.Issuer;
                options.Audience = entraAuthenticationOptions?.ApplicationIdUri;
                options.TokenValidationParameters = new TokenValidationParameters
                {
                    ValidateAudience = true,
                    ValidateIssuer = true,
                };
            })
            .AddJwtBearer("B2C", options =>
            {
                options.Audience = b2CAuthenticationOptions?.ResourceId;
                options.Authority = authority;
            });

        return services;
    }
}

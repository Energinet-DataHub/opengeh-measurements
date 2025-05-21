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
        var azureAdOptions = configuration
            .GetSection(AzureAdAuthenticationOptions.SectionName)
            .Get<AzureAdAuthenticationOptions>();
        var authority = $"https://login.microsoftonline.com/{azureAdOptions?.TenantId}/v2.0";

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
            .AddJwtBearer("AzureAD", options =>
            {
                options.Audience = azureAdOptions?.ResourceId;
                options.Authority = authority;
            });

        return services;
    }
}

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

        // Add authentication for Entra ID
        services.AddAuthentication().AddJwtBearer(JwtBearerDefaults.AuthenticationScheme, options =>
        {
            options.Authority = entraAuthenticationOptions?.Issuer;
            options.Audience = entraAuthenticationOptions?.ApplicationIdUri;
            options.TokenValidationParameters = new TokenValidationParameters
            {
                ValidateAudience = true,
                ValidateIssuer = true,
            };
        });

        // Add authentication for Azure AD
        services.AddAuthentication().AddJwtBearer(JwtBearerDefaults.AuthenticationScheme, options =>
        {
            options.Authority = authority;
            options.Audience = azureAdOptions?.Audience;
        });

        return services;
    }
}

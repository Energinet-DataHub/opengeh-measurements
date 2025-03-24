using Microsoft.IdentityModel.Tokens;

namespace Energinet.DataHub.Measurements.WebApi.Extensions.DependencyInjection;

public static class AuthenticationExtensions
{
    public static IServiceCollection AddAuthenticationForWebApp(this IServiceCollection services, IConfiguration configuration)
    {
        var tenantId = configuration["AzureAd:TenantId"];
        var audience = configuration["AzureAd:ResourceId"];
        var authority = $"https://login.microsoftonline.com/{tenantId}/v2.0";

        services.AddAuthentication().AddJwtBearer("Bearer", options =>
        {
            options.Authority = authority;
            options.TokenValidationParameters = new TokenValidationParameters
            {
                ValidateAudience = true,
                ValidateIssuer = true,
                ValidAudience = audience,
            };
        });
        return services;
    }
}

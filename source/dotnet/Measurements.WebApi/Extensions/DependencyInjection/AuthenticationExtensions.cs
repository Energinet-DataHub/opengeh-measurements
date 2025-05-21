using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Protocols.Configuration;
using Microsoft.IdentityModel.Tokens;
using AuthenticationOptions = Energinet.DataHub.Measurements.Application.Extensions.Options.AuthenticationOptions;

namespace Energinet.DataHub.Measurements.WebApi.Extensions.DependencyInjection;

public static class AuthenticationExtensions
{
    public static IServiceCollection AddAuthenticationForWebApp(this IServiceCollection services, IConfiguration configuration)
    {
        var authenticationOptions = configuration
            .GetSection(AuthenticationOptions.SectionName)
            .Get<AuthenticationOptions>();

        services.AddAuthentication().AddJwtBearer(JwtBearerDefaults.AuthenticationScheme, options =>
        {
            options.Authority = authenticationOptions?.Issuer;
            options.Audience = authenticationOptions?.ApplicationIdUri;
            options.TokenValidationParameters = new TokenValidationParameters
            {
                ValidateAudience = true,
                ValidateIssuer = true,
            };
        });
        return services;
    }
}

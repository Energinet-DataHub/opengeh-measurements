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
            .GetRequiredSection(AuthenticationOptions.SectionName)
            .Get<AuthenticationOptions>();

        GuardAuthenticationOptions(authenticationOptions);

        services.AddAuthentication().AddJwtBearer(JwtBearerDefaults.AuthenticationScheme, options =>
        {
            options.Authority = authenticationOptions!.Issuer;
            options.Audience = authenticationOptions.ApplicationIdUri;
            options.TokenValidationParameters = new TokenValidationParameters
            {
                ValidateAudience = true,
                ValidateIssuer = true,
            };
        });
        return services;
    }

    private static void GuardAuthenticationOptions(AuthenticationOptions? authenticationOptions)
    {
        if (string.IsNullOrWhiteSpace(authenticationOptions?.ApplicationIdUri))
        {
            throw new InvalidConfigurationException($"Missing '{nameof(AuthenticationOptions.ApplicationIdUri)}'.");
        }

        if (string.IsNullOrWhiteSpace(authenticationOptions.Issuer))
        {
            throw new InvalidConfigurationException($"Missing '{nameof(AuthenticationOptions.Issuer)}'.");
        }
    }
}

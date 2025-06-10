using Energinet.DataHub.Core.App.WebApp.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.WebApi.Constants;

namespace Energinet.DataHub.Measurements.WebApi.Extensions.DependencyInjection;

public static class AuthenticationExtensions
{
    public static IServiceCollection AddAuthenticationForWebApp(this IServiceCollection services, IConfiguration configuration)
    {
        var b2CAuthenticationOptions = configuration
            .GetSection(B2CAuthenticationOptions.SectionName)
            .Get<B2CAuthenticationOptions>();

        if (b2CAuthenticationOptions?.TenantId == string.Empty &&
            b2CAuthenticationOptions.ResourceId == string.Empty)
        {
            services.AddSubsystemAuthenticationForWebApp(configuration);
        }
        else
        {
            AddTemporaryAuthenticationUsingB2C(services, b2CAuthenticationOptions!);
        }

        return services;
    }

    private static void AddTemporaryAuthenticationUsingB2C(
        IServiceCollection services,
        B2CAuthenticationOptions b2CAuthenticationOptions)
    {
        var authority = $"https://login.microsoftonline.com/{b2CAuthenticationOptions?.TenantId}/v2.0";

        services.AddAuthentication(AuthenticationSchemas.Default)
            .AddJwtBearer(AuthenticationSchemas.B2C, options =>
            {
                options.Audience = b2CAuthenticationOptions?.ResourceId;
                options.Authority = authority;
            });
    }
}

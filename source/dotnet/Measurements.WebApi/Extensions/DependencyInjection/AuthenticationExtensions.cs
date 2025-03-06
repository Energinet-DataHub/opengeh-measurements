namespace Energinet.DataHub.Measurements.WebApi.Extensions.DependencyInjection;

public static class AuthenticationExtensions
{
    public static IServiceCollection AddAuthenticationForWebApp(this IServiceCollection services, IConfiguration configuration)
    {
        var tenantId = configuration["AzureAd:TenantId"];
        var audience = configuration["AzureAd:ResourceId"];
        var authority = $"https://login.microsoftonline.com/{tenantId}/v2.0";

        services.AddAuthentication()
            .AddJwtBearer(x =>
            {
                x.Audience = audience;
                x.Authority = authority;
            });

        return services;
    }
}

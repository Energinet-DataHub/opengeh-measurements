using Microsoft.AspNetCore.Authorization;

namespace Energinet.DataHub.Measurements.WebApi.Extensions.DependencyInjection;

public static class AuthorizationExtensions
{
    public static IServiceCollection AddAuthorizationForWebApp(this IServiceCollection services)
    {
        services.AddAuthorizationBuilder()
            .AddPolicy("AllowAnonymous", policy =>
            {
                policy.RequireAssertion(_ => true);
            })
            .SetFallbackPolicy(new AuthorizationPolicyBuilder()
                .RequireAuthenticatedUser()
                .Build());
        return services;
    }
}

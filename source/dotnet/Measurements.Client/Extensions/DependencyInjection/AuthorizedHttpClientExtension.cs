using Energinet.DataHub.Measurements.Client.Authentication;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;

public static class AuthorizedHttpClientExtension
{
    public static IServiceCollection AddAuthorizedHttpClient(this IServiceCollection serviceCollection)
    {
        return serviceCollection
            .AddSingleton<IAuthorizedHttpClientFactory>(provider => new AuthorizedHttpClientFactory(
                provider.GetRequiredService<IHttpClientFactory>(),
                () =>
                {
                    var httpContextAccessor = provider.GetRequiredService<IHttpContextAccessor>();
                    return (string?)httpContextAccessor.HttpContext?.Request.Headers.Authorization ?? string.Empty;
                }));
    }
}

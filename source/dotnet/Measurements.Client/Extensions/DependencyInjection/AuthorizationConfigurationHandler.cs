using System.Net.Http.Headers;

namespace Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;

internal sealed class AuthorizationConfigurationHandler(
    IServiceProvider serviceProvider,
    Func<IServiceProvider, Task<AuthenticationHeaderValue>> customAuthorizationHandler)
    : DelegatingHandler
{
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        request.Headers.Authorization = await customAuthorizationHandler(serviceProvider);
        return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
    }
}

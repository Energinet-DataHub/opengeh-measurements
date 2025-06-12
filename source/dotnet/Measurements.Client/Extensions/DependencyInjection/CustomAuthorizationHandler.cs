using System.Net.Http.Headers;

namespace Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;

public class CustomAuthorizationHandler(Func<Task<AuthenticationHeaderValue>> authenticationHeaderProvider)
    : DelegatingHandler
{
    protected sealed override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var authenticationHeader = await authenticationHeaderProvider.Invoke().ConfigureAwait(false);
        request.Headers.Authorization = authenticationHeader;

        return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
    }
}

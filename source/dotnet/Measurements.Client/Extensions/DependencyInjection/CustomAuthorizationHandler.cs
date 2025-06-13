using System.Net.Http.Headers;

namespace Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;

public class CustomAuthorizationHandler(AuthenticationHeaderValue authenticationHeaderValue)
    : DelegatingHandler
{
    protected sealed override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        request.Headers.Authorization = authenticationHeaderValue;
        return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
    }
}

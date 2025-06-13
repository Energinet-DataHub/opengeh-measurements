using System.Net.Http.Headers;

namespace Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;

public class CustomAuthorizationHandler
    : DelegatingHandler
{
    private readonly IServiceProvider _serviceProvider;
    private readonly Func<IServiceProvider, Task<AuthenticationHeaderValue>> _customAuthorizationHandler;

    public CustomAuthorizationHandler(IServiceProvider serviceProvider, Func<IServiceProvider, Task<AuthenticationHeaderValue>> customAuthorizationHandler)
    {
        _serviceProvider = serviceProvider;
        _customAuthorizationHandler = customAuthorizationHandler;
    }

    protected sealed override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        request.Headers.Authorization = await _customAuthorizationHandler(_serviceProvider);
        return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
    }
}

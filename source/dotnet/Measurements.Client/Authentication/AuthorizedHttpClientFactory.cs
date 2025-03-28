using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;

namespace Energinet.DataHub.Measurements.Client.Authentication;

public class AuthorizedHttpClientFactory(
    IHttpClientFactory httpClientFactory,
    Func<string> authorizationHeaderProvider) : IAuthorizedHttpClientFactory
{
    public HttpClient CreateClient()
    {
        var client = httpClientFactory.CreateClient(MeasurementsHttpClientNames.MeasurementsApi);
        SetAuthorizationHeader(client);
        return client;
    }

    private void SetAuthorizationHeader(HttpClient httpClient)
    {
        var authorizationHeader = authorizationHeaderProvider();
        if (string.IsNullOrEmpty(authorizationHeader))
        {
            return;
        }

        httpClient.DefaultRequestHeaders.Add("Authorization", authorizationHeader);
    }
}

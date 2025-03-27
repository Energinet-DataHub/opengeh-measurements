namespace Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;

/// <summary>
/// Factory to create an <see cref="T:System.Net.Http.HttpClient" />, which will re-apply the authorization header
/// from the current HTTP context.
/// </summary>
public class AuthorizedHttpClientFactory(
    IHttpClientFactory httpClientFactory,
    Func<string> authorizationHeaderProvider)
{
    public HttpClient CreateClient(Uri baseUrl)
    {
        var client = httpClientFactory.CreateClient();
        SetAuthorizationHeader(client);
        client.BaseAddress = baseUrl;
        return client;
    }

    private void SetAuthorizationHeader(HttpClient httpClient)
    {
        var str = authorizationHeaderProvider();
        if (string.IsNullOrEmpty(str))
        {
            return;
        }

        httpClient.DefaultRequestHeaders.Add("Authorization", str);
    }
}

namespace Energinet.DataHub.Measurements.Client.Tests.Fixtures;

public class FakeHttpClientFactory(HttpClient httpClient) : IHttpClientFactory
{
    public HttpClient CreateClient(string name) => httpClient;
}

namespace Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;

/// <summary>
/// Factory to create an authorized HttpClient
/// </summary>
public interface IAuthorizedHttpClientFactory
{
    /// <summary>
    /// Method to create an authorized HttpClient.
    /// </summary>
    /// <returns>An authorized HttpClient</returns>
    HttpClient CreateClient();
}

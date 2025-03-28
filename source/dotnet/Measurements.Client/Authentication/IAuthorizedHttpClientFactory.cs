namespace Energinet.DataHub.Measurements.Client.Authentication;

/// <summary>
/// Factory to create an <see cref="T:System.Net.Http.HttpClient" />, which will re-apply the authorization header
/// from the current HTTP context.
/// </summary>
public interface IAuthorizedHttpClientFactory
{
    /// <summary>
    /// Method to create an authorized HttpClient.
    /// </summary>
    /// <returns>An authorized HttpClient</returns>
    HttpClient CreateClient();
}

using System.Net.Http.Headers;

namespace Energinet.DataHub.Measurements.Client.Authentication;

/// <summary>
/// Interface for providing authorization header configurations based on credentials.
/// </summary>
public interface IAuthorizationHeaderProvider
{
    /// <summary>
    /// Create an authorization header to be used when calling Measurement API.
    /// </summary>
    AuthenticationHeaderValue CreateAuthorizationHeader();

    /// <summary>
    /// Create an authorization header to be used when calling Measurement API.
    /// </summary>
    Task<AuthenticationHeaderValue> CreateAuthorizationHeaderAsync();
}

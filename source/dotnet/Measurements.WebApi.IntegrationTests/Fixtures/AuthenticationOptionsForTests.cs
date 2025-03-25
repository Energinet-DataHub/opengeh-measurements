namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;

public static class AuthenticationOptionsForTests
{
    /// <summary>
    /// Uri (scope) for which the client must request a token and send as part of the http request.
    /// In integration tests we use one that we know all identities (developers, CI/CD) can get a token for.
    /// The protected app's use this as Audience in JWT validation.
    /// </summary>
    public const string ApplicationIdUri = "https://management.azure.com";

    /// <summary>
    /// Token issuer.
    /// The protected app's use this as Authority in JWT validation.
    /// </summary>
    public const string Issuer = "https://sts.windows.net/f7619355-6c67-4100-9a78-1847f30742e2/";
}

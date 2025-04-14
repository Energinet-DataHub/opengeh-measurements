using System.Net.Http.Headers;
using Azure.Core;

namespace Energinet.DataHub.Measurements.Client.Authentication;

public class AuthorizationHeaderProvider(TokenCredential credential, string applicationIdUri)
    : IAuthorizationHeaderProvider
{
    public AuthenticationHeaderValue CreateAuthorizationHeader()
    {
        var token = credential
            .GetToken(new TokenRequestContext([applicationIdUri]), CancellationToken.None)
            .Token;

        return new AuthenticationHeaderValue("Bearer", token);
    }
}

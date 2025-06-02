using System.Net.Http.Headers;
using Azure.Core;
using Microsoft.AspNetCore.Authentication.JwtBearer;

namespace Energinet.DataHub.Measurements.Client.Authentication;

public class AuthorizationHeaderProvider(TokenCredential credential, string applicationIdUri)
    : IAuthorizationHeaderProvider
{
    public AuthenticationHeaderValue CreateAuthenticationHeaderValue()
    {
        var token = credential
            .GetToken(new TokenRequestContext([applicationIdUri]), CancellationToken.None)
            .Token;

        return new AuthenticationHeaderValue(JwtBearerDefaults.AuthenticationScheme, token);
    }

    public async Task<AuthenticationHeaderValue> CreateAuthenticationHeaderValueAsync()
    {
        var accessToken = await credential
            .GetTokenAsync(new TokenRequestContext([applicationIdUri]), CancellationToken.None);

        return new AuthenticationHeaderValue(JwtBearerDefaults.AuthenticationScheme, accessToken.Token);
    }
}

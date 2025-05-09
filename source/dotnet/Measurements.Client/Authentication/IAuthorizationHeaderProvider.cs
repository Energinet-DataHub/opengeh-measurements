﻿using System.Net.Http.Headers;

namespace Energinet.DataHub.Measurements.Client.Authentication;

/// <summary>
/// Interface for providing authorization header configurations based on credentials.
/// </summary>
internal interface IAuthorizationHeaderProvider
{
    /// <summary>
    /// Create an authorization header to be used when calling Measurement API.
    /// </summary>
    AuthenticationHeaderValue CreateAuthorizationHeader();
}

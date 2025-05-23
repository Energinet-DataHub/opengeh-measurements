using System.ComponentModel.DataAnnotations;

namespace Energinet.DataHub.Measurements.Application.Extensions.Options;

/// <summary>
/// Contains options for validating the JWT bearer tokens that must be sent as
/// part of any http request for protected http endpoints.
/// </summary>
public class EntraAuthenticationOptions
{
    public const string SectionName = "Auth";

    /// <summary>
    /// Uri (scope) which must match the audience of the token.
    /// </summary>
    [Required(AllowEmptyStrings = false)]
    public string ApplicationIdUri { get; init; } = string.Empty;

    /// <summary>
    /// Issuer (tenant) which must match the issuer of the token.
    /// Also used to configure Authority in JWT validation.
    /// </summary>
    [Required(AllowEmptyStrings = false)]
    public string Issuer { get; init; } = string.Empty;
}

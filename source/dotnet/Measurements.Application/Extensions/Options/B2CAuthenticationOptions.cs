using System.ComponentModel.DataAnnotations;

namespace Energinet.DataHub.Measurements.Application.Extensions.Options;

/// <summary>
/// Contains options for validating the JWT bearer tokens that must be sent as
/// part of any http request for protected http endpoints.
/// </summary>
public class B2CAuthenticationOptions
{
    public const string SectionName = "AzureAd";

    [Required(AllowEmptyStrings = false)]
    public string TenantId { get; init; } = string.Empty;

    [Required(AllowEmptyStrings = false)]
    public string ResourceId { get; init; } = string.Empty;
}

using System.ComponentModel.DataAnnotations;

namespace Energinet.DataHub.Measurements.Application.Extensions.Options;

/// <summary>
/// Contains options for validating the JWT bearer tokens that must be sent as
/// part of any http request for protected http endpoints.
/// </summary>
public class AzureAdAuthenticationOptions
{
    public const string SectionName = "AzureAD";

    [Required(AllowEmptyStrings = false)]
    public string TenantId { get; init; } = string.Empty;

    [Required(AllowEmptyStrings = false)]
    public string Audience { get; init; } = string.Empty;

    [Required(AllowEmptyStrings = false)]
    public string Authority { get; init; } = string.Empty;
}

using System.ComponentModel.DataAnnotations;

namespace Energinet.DataHub.Measurements.Client.Extensions.Options;

public class MeasurementHttpClientOptions
{
    public const string SectionName = "MeasurementsHttpClient";

    [Required(AllowEmptyStrings = false)]
    public string BaseAddress { get; init; } = string.Empty;
}

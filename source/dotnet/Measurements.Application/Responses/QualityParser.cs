using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public sealed class QualityParser
{
    public static Quality ParseQuality(string quality)
    {
        return quality.ToLower() switch
        {
            "missing" => Quality.Missing,
            "estimated" => Quality.Estimated,
            "measured" => Quality.Measured,
            "calculated" => Quality.Calculated,
            _ => throw new ArgumentOutOfRangeException(nameof(quality)),
        };
    }
}

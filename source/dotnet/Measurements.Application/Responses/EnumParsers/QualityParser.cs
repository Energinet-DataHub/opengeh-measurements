using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses.EnumParsers;

public static class QualityParser
{
    public static Quality ParseQuality(string quality)
    {
        return quality.ToLower() switch
        {
            "missing" => Quality.Missing,
            "estimated" => Quality.Estimated,
            "calculated" => Quality.Calculated,
            "measured" => Quality.Measured,
            _ => throw new ArgumentOutOfRangeException(nameof(quality)),
        };
    }
}

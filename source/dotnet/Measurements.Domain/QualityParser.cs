namespace Energinet.DataHub.Measurements.Domain;

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

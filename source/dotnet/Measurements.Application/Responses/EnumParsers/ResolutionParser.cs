using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses.EnumParsers;

public static class ResolutionParser
{
    public static Resolution ParseResolution(string resolution)
    {
        return resolution.ToUpper() switch
        {
            "PT15M" => Resolution.QuarterHourly,
            "PT1H" => Resolution.Hourly,
            "P1D" => Resolution.Daily,
            "P1M" => Resolution.Monthly,
            "P1Y" => Resolution.Yearly,
            _ => throw new ArgumentOutOfRangeException(nameof(resolution)),
        };
    }
}

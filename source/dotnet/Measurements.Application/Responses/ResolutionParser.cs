using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public sealed class ResolutionParser
{
    public static Resolution ParseResolution(string resolution)
    {
        return resolution.ToUpper() switch
        {
            "PT15M" => Resolution.PT15M,
            "PT1H" => Resolution.PT1H,
            "P1D" => Resolution.P1D,
            "P1M" => Resolution.P1M,
            "P1Y" => Resolution.P1Y,
            _ => throw new ArgumentOutOfRangeException(nameof(resolution)),
        };
    }
}

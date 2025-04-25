using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public static class PointCountCalculator
{
    public static int GetExpectedPointCount(Resolution resolution, int hours)
    {
        var expectedPointCount = resolution switch
        {
            Resolution.QuarterHourly => hours * 4,
            Resolution.Hourly => hours,
            Resolution.Daily or Resolution.Monthly or Resolution.Yearly => 1,
            _ => throw new ArgumentOutOfRangeException(resolution.ToString()),
        };
        return expectedPointCount;
    }
}

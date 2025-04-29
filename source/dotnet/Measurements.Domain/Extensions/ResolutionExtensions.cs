using NodaTime;

namespace Energinet.DataHub.Measurements.Domain.Extensions;

public static class ResolutionExtensions
{
    public static int GetExpectedPointCount(this Resolution resolution, Instant maxObservationTime, Instant minObservationTime)
    {
        var hours = GetHoursForAggregation(maxObservationTime, minObservationTime);

        var expectedPointCount = resolution switch
        {
            Resolution.QuarterHourly => hours * 4,
            Resolution.Hourly => hours,
            Resolution.Daily or Resolution.Monthly or Resolution.Yearly => 1,
            _ => throw new ArgumentOutOfRangeException(resolution.ToString()),
        };
        return expectedPointCount;
    }

    private static int GetHoursForAggregation(Instant maxObservationTime, Instant minObservationTime)
    {
        var timeSpan = maxObservationTime - minObservationTime;
        var hours = (int)timeSpan.TotalHours + 1;
        return hours;
    }
}

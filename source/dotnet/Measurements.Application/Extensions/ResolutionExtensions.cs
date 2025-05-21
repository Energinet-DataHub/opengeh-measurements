using Energinet.DataHub.Measurements.Domain;
using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Extensions;

public static class ResolutionExtensions
{
    public static int GetExpectedPointsForPeriod(this Resolution resolution, Instant timestamp, int daysInPeriod)
    {
        var from = timestamp.AtStartOfLocaleDate();
        var to = from.AddLocalDays(daysInPeriod);

        var hours = GetHoursForAggregation(from, to);

        var expectedPointCount = resolution switch
        {
            Resolution.QuarterHourly => hours * 4,
            Resolution.Hourly => hours,
            Resolution.Daily or Resolution.Monthly or Resolution.Yearly => 1,
            _ => throw new ArgumentOutOfRangeException(resolution.ToString()),
        };
        return expectedPointCount;
    }

    private static int GetHoursForAggregation(Instant from, Instant to)
    {
        var timeSpan = to - from;
        return (int)timeSpan.TotalHours;
    }
}

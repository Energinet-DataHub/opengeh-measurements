using Energinet.DataHub.Measurements.Domain;
using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Extensions;

public static class ResolutionExtensions
{
    public static int GetExpectedPointCount(this Resolution resolution, Instant timestamp)
    {
        var localDate = timestamp.ToDanishZonedDateTime().Date;

        var startOfDay = localDate.At(LocalTime.Midnight);
        var startOfNextDay = localDate.PlusDays(1).At(LocalTime.Midnight);

        var zonedStart = startOfDay.ToDanishZonedDateTime();
        var zonedEnd = startOfNextDay.ToDanishZonedDateTime();

        var duration = zonedEnd.ToInstant() - zonedStart.ToInstant();

        return resolution switch
        {
            Resolution.Hourly => (int)duration.TotalHours,
            Resolution.QuarterHourly => (int)(duration.TotalMinutes / 15),
            Resolution.Daily => (int)duration.TotalDays,
            Resolution.Monthly => 0,
            Resolution.Yearly => 0,
            _ => throw new ArgumentOutOfRangeException(nameof(resolution), "Unsupported resolution"),
        };
    }
}

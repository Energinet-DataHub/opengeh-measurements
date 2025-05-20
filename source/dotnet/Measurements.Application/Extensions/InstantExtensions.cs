using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Extensions;

public static class InstantExtensions
{
    private static readonly DateTimeZone _danishZone = DateTimeZoneProviders.Tzdb["Europe/Copenhagen"];

    public static DateOnly ToDateOnly(this Instant instant)
    {
        var localDateTime = instant.InZone(_danishZone).LocalDateTime;
        return new DateOnly(localDateTime.Year, localDateTime.Month, localDateTime.Day);
    }

    public static Instant AddLocalDays(this Instant timestamp, int days)
    {
        return timestamp
            .InZone(_danishZone)
            .Date
            .At(LocalTime.Midnight)
            .PlusDays(days)
            .InZoneLeniently(_danishZone)
            .ToInstant();
    }

    public static Instant AtStartOfLocaleDate(this Instant timestamp)
    {
        return timestamp
            .InZone(_danishZone)
            .LocalDateTime
            .Date
            .AtStartOfDayInZone(_danishZone)
            .ToInstant();
    }

    public static ZonedDateTime ToDanishZonedDateTime(this Instant timestamp)
    {
        return timestamp.InZone(_danishZone);
    }
}

using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Extensions;

public static class NodaTimeExtensions
{
    private static readonly DateTimeZone _danishZone = DateTimeZoneProviders.Tzdb["Europe/Copenhagen"];

    public static DateOnly ToDateOnly(this Instant instant)
    {
        var localDateTime = instant.InZone(_danishZone).LocalDateTime;
        return new DateOnly(localDateTime.Year, localDateTime.Month, localDateTime.Day);
    }

    public static ZonedDateTime ToDanishZonedDateTime(this Instant date)
    {
        return date.InZone(_danishZone);
    }

    public static ZonedDateTime ToDanishZonedDateTime(this LocalDateTime instant)
    {
        return instant.InZoneLeniently(_danishZone);
    }
}

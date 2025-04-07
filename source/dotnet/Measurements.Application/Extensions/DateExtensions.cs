using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Extensions;

public static class DateExtensions
{
    private static readonly DateTimeZone _danishZone = DateTimeZoneProviders.Tzdb["Europe/Copenhagen"];

    public static DateOnly ToDateOnly(this Instant instant)
    {
        var localDateTime = instant.InZone(_danishZone).LocalDateTime;
        return new DateOnly(localDateTime.Year, localDateTime.Month, localDateTime.Day);
    }
}

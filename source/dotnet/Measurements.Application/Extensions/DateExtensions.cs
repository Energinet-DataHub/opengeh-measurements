using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Extensions;

public static class DateExtensions
{
    private static readonly DateTimeZone _danishZone = DateTimeZoneProviders.Tzdb["Europe/Copenhagen"];

    public static LocalDate ToLocalDate(this Instant instant)
    {
        var localDateTime = instant.InZone(_danishZone).LocalDateTime;
        return localDateTime.Date;
    }
}

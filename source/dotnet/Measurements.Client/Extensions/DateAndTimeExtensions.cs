using System.Globalization;
using NodaTime;

namespace Energinet.DataHub.Measurements.Client.Extensions;

public static class DateAndTimeExtensions
{
    private const string Format = "yyyy-MM-ddTHH:mm:ss'Z'";
    private static readonly DateTimeZone _danishZone = DateTimeZoneProviders.Tzdb["Europe/Copenhagen"];

    public static string ToUtcString(this LocalDate date)
    {
        return date
            .AtMidnight()
            .InZoneLeniently(_danishZone)
            .ToInstant()
            .ToString(Format, CultureInfo.InvariantCulture);
    }

    public static DateTimeOffset ToUtcDateTimeOffset(this LocalDate date)
    {
        return date
            .AtMidnight()
            .InZoneLeniently(_danishZone)
            .ToInstant()
            .ToDateTimeOffset();
    }

    public static LocalDate ToLocalDate(this DateTimeOffset dateTimeOffset)
    {
        var instant = Instant.FromDateTimeOffset(dateTimeOffset);
        var localDateTime = instant.InZone(_danishZone).LocalDateTime;
        return localDateTime.Date;
    }

    public static string ToFormattedString(this DateTimeOffset date)
    {
        return date.ToString(Format, CultureInfo.InvariantCulture);
    }
}

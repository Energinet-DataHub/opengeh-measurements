using System.Globalization;
using NodaTime;

namespace Energinet.DataHub.Measurements.Infrastructure.Extensions;

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

    public static DateTimeOffset ToDateTimeOffSet(this LocalDate date)
    {
        return date
            .AtMidnight()
            .InZoneLeniently(_danishZone)
            .ToDateTimeOffset();
    }

    public static string ToUtcString(this Instant date)
    {
        var localDate = date.InUtc().Date;
        return localDate.ToUtcString();
    }
}

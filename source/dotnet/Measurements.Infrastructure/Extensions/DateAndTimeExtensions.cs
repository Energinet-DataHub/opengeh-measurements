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

    public static DateTimeOffset ToDateTimeOffSetAtMidnight(this LocalDate date)
    {
        return date
            .AtMidnight()
            .InZoneLeniently(_danishZone)
            .ToDateTimeOffset();
    }

    public static Instant ToInstantAtMidnight(this LocalDate date)
    {
        return date
            .AtMidnight()
            .InZoneLeniently(_danishZone)
            .ToInstant();
    }

    public static (LocalDate Start, LocalDate End) ToDateIntervalIncludingLastDay(this YearMonth yearMonth)
    {
        var (start, end) = yearMonth.ToDateInterval();
        return (start, end.PlusDays(1));
    }

    public static string ToUtcString(this Instant date)
    {
        var localDate = date.InUtc().Date;
        return localDate.ToUtcString();
    }
}

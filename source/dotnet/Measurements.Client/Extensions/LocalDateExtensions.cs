using System.Globalization;
using NodaTime;

namespace Energinet.DataHub.Measurements.Client.Extensions;

public static class LocalDateExtensions
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
}

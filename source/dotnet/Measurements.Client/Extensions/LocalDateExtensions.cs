using System.Globalization;
using NodaTime;
using NodaTime.Extensions;

namespace Energinet.DataHub.Measurements.Client.Extensions;

public static class LocalDateExtensions
{
    private const string Format = "yyyy-MM-ddTHH:mm:ss'Z'";
    private static readonly DateTimeZone _danishZone = DateTimeZoneProviders.Tzdb["Europe/Copenhagen"];

    public static string ToUtcString(this DateOnly date)
    {
        var utcDateTime = date
            .ToLocalDate()
            .AtMidnight()
            .InZoneLeniently(_danishZone)
            .ToInstant();

        return utcDateTime.ToString(Format, CultureInfo.InvariantCulture);
    }

    public static DateTimeOffset ToUtcDateTimeOffset(this DateOnly date)
    {
        return date
            .ToLocalDate()
            .AtMidnight()
            .InZoneLeniently(_danishZone)
            .ToInstant()
            .ToDateTimeOffset();
    }
}

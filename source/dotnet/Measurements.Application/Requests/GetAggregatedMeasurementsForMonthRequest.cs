namespace Energinet.DataHub.Measurements.Application.Requests;

public class GetAggregatedMeasurementsForMonthRequest(string meteringPointId, int year, int month)
{
    private static bool IsValidYear(int year)
    {
        return year is >= -9998 and <= 9999;
    }

    private static bool IsValidMonth(int month)
    {
        return month is >= 1 and <= 12;
    }

    public string MeteringPointId { get; } = meteringPointId;

    public int Year { get; } = IsValidYear(year) ? year :
        throw new ArgumentOutOfRangeException(nameof(year), "Year must be between -9998 and 9999");

    public int Month { get; } = IsValidMonth(month) ? month :
        throw new ArgumentOutOfRangeException(nameof(month), "Month must be between 1 and 12");
}

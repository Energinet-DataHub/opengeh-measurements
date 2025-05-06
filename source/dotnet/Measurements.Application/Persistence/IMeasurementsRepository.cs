using Energinet.DataHub.Measurements.Domain;
using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Persistence;

/// <summary>
/// Repository for fetching measurements.
/// </summary>
public interface IMeasurementsRepository
{
    /// <summary>
    /// Get measurements for a given metering point in period defined by from and to timestamps.
    /// </summary>
    IAsyncEnumerable<MeasurementResult> GetByPeriodAsync(string meteringPointId, Instant from, Instant to);

    /// <summary>
    /// Get measurements aggregated by date for a given metering point and month.
    /// </summary>
    IAsyncEnumerable<AggregatedMeasurementsResult> GetAggregatedByDateAsync(string meteringPointId, YearMonth yearMonth);

    /// <summary>
    /// Get measurements aggregated by month for a given metering point and year.
    /// </summary>
    IAsyncEnumerable<AggregatedMeasurementsResult> GetAggregatedByMonthAsync(string meteringPointId, Year year);

    /// <summary>
    /// Get measurements aggregated by period for a given metering point and period.
    /// </summary>
    IAsyncEnumerable<AggregatedMeasurementsResult> GetAggregatedByPeriodAsync(string meteringPointIds, Instant from, Instant to, Aggregation aggregation);

    /// <summary>
    /// Get measurements aggregated by year for a given metering point.
    /// </summary>
    IAsyncEnumerable<AggregatedMeasurementsResult> GetAggregatedByYearAsync(string meteringPointId);
}

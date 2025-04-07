using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;

namespace Energinet.DataHub.Measurements.Client;

/// <summary>
/// Client for using the Measurement API.
/// </summary>
public interface IMeasurementsClient
{
    /// <summary>
    /// Get measurements for a specific day.
    /// </summary>
    Task<IEnumerable<MeasurementPointDto>> GetMeasurementsForDayAsync(GetMeasurementsForDayQuery query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get measurements for a specified period.
    /// </summary>
    Task<IEnumerable<MeasurementPointDto>> GetMeasurementsForPeriodAsync(GetMeasurementsForPeriodQuery query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get aggregated measurements for a specified month.
    /// </summary>
    Task<IEnumerable<MeasurementAggregationDto>> GetAggregatedMeasurementsForMonth(GetAggregatedMeasurementsForMonthQuery query, CancellationToken cancellationToken = default);
}

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
    Task<IEnumerable<MeasurementPointDto>> GetByDayAsync(GetByDayQuery query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get measurements for a specified period.
    /// </summary>
    Task<IEnumerable<MeasurementPointDto>> GetByPeriodAsync(GetByPeriodQuery query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get aggregated measurements for a specified month.
    /// </summary>
    Task<IEnumerable<MeasurementAggregationDto>> GetAggregatedByMonth(GetAggregatedByMonthQuery query, CancellationToken cancellationToken = default);
}

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
    Task<MeasurementDto> GetByDayAsync(GetByDayQuery query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get current measurements for a specified period.
    /// </summary>
    Task<CurrentMeasurementDto> GetByPeriodAsync(GetByPeriodQuery query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get measurements aggregated by date for a specified month.
    /// </summary>
    Task<IEnumerable<MeasurementAggregationByDateDto>> GetMonthlyAggregateByDateAsync(GetMonthlyAggregateByDateQuery query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get measurements aggregated by month for a specified year.
    /// </summary>
    Task<IEnumerable<MeasurementAggregationByMonthDto>> GetYearlyAggregateByMonthAsync(GetYearlyAggregateByMonthQuery query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get measurements aggregated by year for all years.
    /// </summary>
    Task<IEnumerable<MeasurementAggregationByYearDto>> GetAggregateByYearAsync(GetAggregateByYearQuery query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get measurements aggregated for a specified period.
    /// </summary>
    Task<MeasurementAggregationByPeriodDto> GetAggregateByPeriodAsync(GetAggregateByPeriodQuery query, CancellationToken cancellationToken = default);
}

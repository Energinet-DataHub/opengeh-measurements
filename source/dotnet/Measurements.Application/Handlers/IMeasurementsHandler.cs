using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Application.Responses;

namespace Energinet.DataHub.Measurements.Application.Handlers;

/// <summary>
/// Interface for handling measurements requests.
/// </summary>
public interface IMeasurementsHandler
{
    /// <summary>
    /// Get measurements matching request attributes.
    /// </summary>
    /// <param name="getByPeriodRequest"></param>
    Task<MeasurementsResponse> GetByPeriodAsync(GetByPeriodRequest getByPeriodRequest);

    /// <summary>
    /// Get measurements aggregated by date matching request attributes.
    /// </summary>
    /// <param name="getAggregatedByDateRequest"></param>
    Task<MeasurementsAggregatedByDateResponse> GetAggregatedByDateAsync(GetAggregatedByDateRequest getAggregatedByDateRequest);

    /// <summary>
    /// Get measurements aggregated by month matching request attributes.
    /// </summary>
    /// <param name="getAggregatedByMonthRequest"></param>
    Task<MeasurementsAggregatedByMonthResponse> GetAggregatedByMonthAsync(GetAggregatedByMonthRequest getAggregatedByMonthRequest);

    /// <summary>
    /// Get aggregated measurements matching request attributes.
    /// </summary>
    /// <param name="getAggregatedByPeriodRequest"></param>
    Task<MeasurementsAggregatedByPeriodResponse> GetAggregatedByPeriodAsync(
        GetAggregatedByPeriodRequest getAggregatedByPeriodRequest);

    /// <summary>
    /// Get measurements aggregated by year matching request attributes.
    /// </summary>
    /// <param name="getAggregatedByYearRequest"></param>
    Task<MeasurementsAggregatedByYearResponse> GetAggregatedByYearAsync(GetAggregatedByYearRequest getAggregatedByYearRequest);
}

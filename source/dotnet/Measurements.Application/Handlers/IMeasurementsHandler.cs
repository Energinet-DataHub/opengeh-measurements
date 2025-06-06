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
    [Obsolete("Use GetAggregatedByDateAsync instead.")]
    Task<MeasurementsAggregatedByDateResponseV4> GetAggregatedByDateAsyncV4(GetAggregatedByDateRequest getAggregatedByDateRequest);

    /// <summary>
    /// Get measurements aggregated by date matching request attributes.
    /// </summary>
    /// <param name="getAggregatedByDateRequest"></param>
    Task<MeasurementsAggregatedByDateResponse> GetAggregatedByDateAsync(GetAggregatedByDateRequest getAggregatedByDateRequest);

    /// <summary>
    /// Get measurements aggregated by month matching request attributes.
    /// </summary>
    /// <param name="getAggregatedByMonthRequest"></param>
    [Obsolete("Use GetAggregatedByMonthAsync instead.")]
    Task<MeasurementsAggregatedByMonthResponseV4> GetAggregatedByMonthAsyncV4(GetAggregatedByMonthRequest getAggregatedByMonthRequest);

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
    [Obsolete("Use GetAggregatedByYearAsync instead.")]
    Task<MeasurementsAggregatedByYearResponseV4> GetAggregatedByYearAsyncV4(GetAggregatedByYearRequest getAggregatedByYearRequest);

    /// <summary>
    /// Get measurements aggregated by year matching request attributes.
    /// </summary>
    /// <param name="getAggregatedByYearRequest"></param>
    Task<MeasurementsAggregatedByYearResponse> GetAggregatedByYearAsync(GetAggregatedByYearRequest getAggregatedByYearRequest);
}

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
    Task<GetMeasurementResponse> GetByPeriodAsync(GetByPeriodRequest getByPeriodRequest);

    /// <summary>
    /// Get measurements aggregated by date matching request attributes.
    /// </summary>
    /// <param name="getAggregatedByDateRequest"></param>
    Task<GetMeasurementsAggregatedByDateResponse> GetAggregatedByDateAsync(GetAggregatedByDateRequest getAggregatedByDateRequest);

    /// <summary>
    /// Get measurements aggregated by month matching request attributes.
    /// </summary>
    /// <param name="getAggregatedByMonthRequest"></param>
    Task<GetMeasurementsAggregatedByMonthResponse> GetAggregatedByMonthAsync(GetAggregatedByMonthRequest getAggregatedByMonthRequest);

    /// <summary>
    /// Get aggregated measurements matching request attributes.
    /// </summary>
    /// <param name="getAggregatedByPeriodRequest"></param>
    Task<GetAggregatedMeasurementsResponse> GetAggregatedByPeriodAsync(
        GetAggregatedByPeriodRequest getAggregatedByPeriodRequest);
}

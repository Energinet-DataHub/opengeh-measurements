using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Application.Responses;

namespace Energinet.DataHub.Measurements.Application.Handlers;

/// <summary>
/// Interface for handling measurements requests.
/// </summary>
public interface IMeasurementsHandler
{
    /// <summary>
    /// Get current measurements matching request attributes.
    /// </summary>
    /// <param name="getByPeriodRequest"></param>
    Task<GetMeasurementResponse> GetByPeriodAsyncV1(GetByPeriodRequest getByPeriodRequest);

    /// <summary>
    /// Get measurements matching request attributes.
    /// </summary>
    /// <param name="getByPeriodRequest"></param>
    Task<GetMeasurementResponse> GetByPeriodAsync(GetByPeriodRequest getByPeriodRequest);

    /// <summary>
    /// Get aggregated measurements matching request attributes.
    /// </summary>
    /// <param name="getAggregatedByMonthRequest"></param>
    Task<GetAggregatedMeasurementsResponse> GetAggregatedByMonthAsync(
        GetAggregatedByMonthRequest getAggregatedByMonthRequest);

    /// <summary>
    /// Get aggregated measurements matching request attributes.
    /// </summary>
    /// <param name="getAggregatedByPeriodRequest"></param>
    Task<GetAggregatedMeasurementsResponse> GetAggregatedByPeriodAsync(
        GetAggregatedByPeriodRequest getAggregatedByPeriodRequest);
}

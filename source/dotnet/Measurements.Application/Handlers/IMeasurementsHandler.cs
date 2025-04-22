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
    /// Get measurements aggregated by month matching request attributes.
    /// </summary>
    /// <param name="getAggregatedByMonthRequest"></param>
    Task<GetAggregatedMeasurementsResponse> GetAggregatedByMonthAsync(GetAggregatedByMonthRequest getAggregatedByMonthRequest);

    /// <summary>
    /// Get measurements aggregated by year matching request attributes.
    /// </summary>
    /// <param name="getAggregatedByYearRequest"></param>
    Task<GetAggregatedMeasurementsResponse> GetAggregatedByYearAsync(GetAggregatedByYearRequest getAggregatedByYearRequest);
}

﻿using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Application.Responses;

namespace Energinet.DataHub.Measurements.Application.Handlers;

/// <summary>
/// Interface for handling measurements requests.
/// </summary>
public interface IMeasurementsHandler
{
    /// <summary>
    /// Get measurement matching request attributes.
    /// </summary>
    /// <param name="getMeasurementRequest"></param>
    Task<GetMeasurementResponse> GetMeasurementAsync(GetMeasurementRequest getMeasurementRequest);

    /// <summary>
    /// Get aggregated measurements matching request attributes.
    /// </summary>
    /// <param name="getAggregatedMeasurementsForMonthRequest"></param>
    Task<GetAggregatedMeasurementsResponse> GetAggregatedMeasurementsAsync(
        GetAggregatedMeasurementsForMonthRequest getAggregatedMeasurementsForMonthRequest);
}

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
    Task<IEnumerable<MeasurementPoint>> GetMeasurementsForDayAsync(GetMeasurementsForDayQuery query, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get measurements for a specific period.
    /// </summary>
    Task<IEnumerable<MeasurementPoint>> GetMeasurementsForPeriodAsync(GetMeasurementsForPeriodQuery query, CancellationToken cancellationToken = default);
}

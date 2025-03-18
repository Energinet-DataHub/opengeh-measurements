using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;

namespace Energinet.DataHub.Measurements.Client;

/// <summary>
/// Client for using the Measurement API.
/// </summary>
public interface IMeasurementsClient
{
    /// <summary>
    /// Get measurement for a specific day.
    /// </summary>
    /// <returns>Measurements for the specified date.</returns>
    Task<MeasurementDto?> GetMeasurementsForDayAsync(GetMeasurementsForDayQuery query, CancellationToken cancellationToken = default);
}

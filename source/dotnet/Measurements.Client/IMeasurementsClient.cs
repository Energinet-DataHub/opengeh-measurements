using Measurements.Abstractions.Api.Dtos;
using Measurements.Abstractions.Api.Queries;

namespace Energinet.DataHub.Measurements.Client;

/// <summary>
/// Client for using the Measurement API.
/// </summary>
public interface IMeasurementsClient
{
    /// <summary>
    /// Get measurement for a specific day.
    /// </summary>
    /// <returns>Measurement for the specified day. Null if no measurements exists.</returns>
    Task<MeasurementDto?> GetMeasurementAsync(GetMeasurementsForDayQuery query, CancellationToken cancellationToken);
}

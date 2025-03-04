using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Handlers;

/// <summary>
/// Interface for handling measurements requests.
/// </summary>
public interface IMeasurementsHandler
{
    /// <summary>
    /// Get measurement by id.
    /// </summary>
    /// <param name="meteringPointId">Id of measurement to fetch.</param>
    /// <param name="startDate"></param>
    /// <param name="endDate"></param>
    /// <returns>Forty-two.</returns>
    Task<Measurement> GetMeasurementAsync(
        string meteringPointId,
        DateTimeOffset startDate,
        DateTimeOffset endDate);
}

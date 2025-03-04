namespace Energinet.DataHub.Measurements.Application.Handlers;

/// <summary>
/// Interface for handling measurements requests.
/// </summary>
public interface IMeasurementsHandler
{
    /// <summary>
    /// Get measurement by id.
    /// </summary>
    /// <param name="measurementId">Id of measurement to fetch.</param>
    /// <returns>Forty-two.</returns>
    Task<int> GetMeasurementAsync(string measurementId);
}

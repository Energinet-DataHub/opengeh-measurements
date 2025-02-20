namespace Energinet.DataHub.Measurements.Application.Handlers;

/// <summary>
/// Interface for handling measurements requests.
/// </summary>
public interface IMeasurementsHandler
{
    /// <summary>
    /// Ya'll got any more of them measurements?.
    /// </summary>
    /// <param name="measurementId">Id of measurement to fetch.</param>
    /// <returns>Forty-two.</returns>
    Task<int> GetMeasurementAsync(string measurementId);
}

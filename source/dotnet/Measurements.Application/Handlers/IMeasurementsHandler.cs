using Energinet.DataHub.Measurements.Application.Responses;
using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Handlers;

/// <summary>
/// Interface for handling measurements requests.
/// </summary>
public interface IMeasurementsHandler
{
    /// <summary>
    /// Get measurement by id and period.
    /// </summary>
    /// <param name="meteringPointId"></param>
    /// <param name="startDate"></param>
    /// <param name="endDate"></param>
    /// <returns>Forty-two.</returns>
    Task<GetMeasurementResponse> GetMeasurementAsync(string meteringPointId, Instant startDate, Instant endDate);
}

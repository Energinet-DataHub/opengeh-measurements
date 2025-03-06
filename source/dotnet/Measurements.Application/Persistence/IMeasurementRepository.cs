using Energinet.DataHub.Measurements.Domain;
using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Persistence;

/// <summary>
/// Repository for fetching measurements.
/// </summary>
public interface IMeasurementRepository
{
    /// <summary>
    /// Get measurement for a given metering point in period defined by from and to timestamps.
    /// </summary>
    /// <param name="meteringPointId"></param>
    /// <param name="from"></param>
    /// <param name="to"></param>
    Task<Measurement> GetMeasurementAsync(string meteringPointId, Instant from, Instant to);
}

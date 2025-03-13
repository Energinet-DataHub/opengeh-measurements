using NodaTime;

namespace Energinet.DataHub.Measurements.Application.Persistence;

/// <summary>
/// Repository for fetching measurements.
/// </summary>
public interface IMeasurementsRepository
{
    /// <summary>
    /// Get measurements for a given metering point in period defined by from and to timestamps.
    /// </summary>
    /// <param name="meteringPointId"></param>
    /// <param name="from"></param>
    /// <param name="to"></param>
    IAsyncEnumerable<MeasurementsResult> GetMeasurementsAsync(string meteringPointId, Instant from, Instant to);
}

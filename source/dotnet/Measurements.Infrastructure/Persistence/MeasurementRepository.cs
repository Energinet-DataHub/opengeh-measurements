using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Domain;
using NodaTime;

namespace Energinet.DataHub.Measurements.Infrastructure.Persistence;

public class MeasurementRepository : IMeasurementRepository
{
    public Task<Measurement> GetMeasurementAsync(string meteringPointId, Instant from, Instant to)
    {
        throw new NotImplementedException();
    }
}

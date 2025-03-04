using Energinet.DataHub.Measurements.Application.Handlers;

namespace Energinet.DataHub.Measurements.Infrastructure.Handlers;

public class MeasurementsHandler : IMeasurementsHandler
{
    public async Task<int> GetMeasurementAsync(string measurementId)
    {
        return await Task.FromResult(42);
    }
}

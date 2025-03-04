using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Infrastructure.Handlers;

public class MeasurementsHandler : IMeasurementsHandler
{
    public async Task<Measurement> GetMeasurementAsync(
        string meteringPointId,
        DateTimeOffset startDate,
        DateTimeOffset endDate)
    {
        return await Task.FromResult(new Measurement(
        [
            new Transaction(
                startDate,
                endDate,
                42,
                "MWh",
                "Quality"),
        ]));
    }
}

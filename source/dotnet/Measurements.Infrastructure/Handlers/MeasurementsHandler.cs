using Energinet.DataHub.Measurements.Application.Dtos;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using NodaTime;

namespace Energinet.DataHub.Measurements.Infrastructure.Handlers;

public class MeasurementsHandler : IMeasurementsHandler
{
    public async Task<GetMeasurementResponse> GetMeasurementAsync(string meteringPointId, Instant startDate, Instant endDate)
    {
        var dummyMeasurement = new Measurement(
            meteringPointId,
            Unit.KWh,
            [new Point(SystemClock.Instance.GetCurrentInstant(), 42, Quality.Measured)]);

        var response = new GetMeasurementResponse(
            dummyMeasurement.MeteringPointId,
            dummyMeasurement.Unit.ToString(),
            dummyMeasurement.Points.Select(x => new PointDto(x.ObservationTime, x.Quantity, x.Quality.ToString())));

        return await Task.FromResult(response);
    }
}

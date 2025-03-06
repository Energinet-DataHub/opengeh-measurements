using Energinet.DataHub.Measurements.Application.Dtos;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Application.Responses;

namespace Energinet.DataHub.Measurements.Infrastructure.Handlers;

public class MeasurementsHandler(IMeasurementRepository measurementRepository)
    : IMeasurementsHandler
{
    public async Task<GetMeasurementResponse> GetMeasurementAsync(GetMeasurementRequest request)
    {
        var foundMeasurement = await measurementRepository.GetMeasurementAsync(
            request.MeteringPointId,
            request.StartDate,
            request.EndDate);

        var response = new GetMeasurementResponse(
            foundMeasurement.MeteringPointId,
            foundMeasurement.Unit.ToString(),
            foundMeasurement.Points.Select(x => new PointDto(x.ObservationTime, x.Quantity, x.Quality.ToString())));

        return await Task.FromResult(response);
    }
}

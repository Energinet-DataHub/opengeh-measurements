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
        var foundMeasurements = measurementRepository.GetMeasurementAsync(
            request.MeteringPointId,
            request.StartDate,
            request.EndDate) ?? throw new Exception("No measurement found for metering point during period");

        return await GetMeasurementResponse.CreateAsync(foundMeasurements);
    }
}

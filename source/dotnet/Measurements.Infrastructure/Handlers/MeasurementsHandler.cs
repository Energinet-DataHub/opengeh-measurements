using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Application.Responses;
using NodaTime.Extensions;

namespace Energinet.DataHub.Measurements.Infrastructure.Handlers;

public class MeasurementsHandler(IMeasurementsRepository measurementsRepository)
    : IMeasurementsHandler
{
    public async Task<GetMeasurementResponse> GetMeasurementAsync(GetMeasurementRequest request)
    {
        var foundMeasurements = await measurementsRepository
            .GetMeasurementsAsync(request.MeteringPointId, request.StartDate.ToInstant(), request.EndDate.ToInstant())
            .ToListAsync() ?? throw new MeasurementsNotFoundDuringPeriodException();

        return GetMeasurementResponse.Create(foundMeasurements);
    }
}

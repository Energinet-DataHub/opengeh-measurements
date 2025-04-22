using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Application.Handlers;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Requests;
using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using NodaTime;
using NodaTime.Extensions;

namespace Energinet.DataHub.Measurements.Infrastructure.Handlers;

public class MeasurementsHandler(IMeasurementsRepository measurementsRepository)
    : IMeasurementsHandler
{
    public async Task<GetMeasurementResponse> GetByPeriodAsyncV1(GetByPeriodRequest request)
    {
        var foundMeasurements = await measurementsRepository
            .GetByPeriodAsyncV1(request.MeteringPointId, request.StartDate.ToInstant(), request.EndDate.ToInstant())
            .ToListAsync() ?? throw new MeasurementsNotFoundDuringPeriodException();

        return GetMeasurementResponse.Create(foundMeasurements);
    }

    public async Task<GetMeasurementResponse> GetByPeriodAsync(GetByPeriodRequest request)
    {
        var foundMeasurements = await measurementsRepository
            .GetByPeriodAsync(request.MeteringPointId, request.StartDate.ToInstant(), request.EndDate.ToInstant())
            .ToListAsync() ?? throw new MeasurementsNotFoundDuringPeriodException();

        return GetMeasurementResponse.Create(foundMeasurements);
    }

    public async Task<GetAggregatedMeasurementsResponse> GetAggregatedByMonthAsync(GetAggregatedByMonthRequest request)
    {
        var yearMonth = new YearMonth(request.Year, request.Month);
        var aggregatedMeasurements = await measurementsRepository
            .GetAggregatedByMonthAsync(request.MeteringPointId, yearMonth)
            .ToListAsync() ?? throw new MeasurementsNotFoundDuringPeriodException();

        return GetAggregatedMeasurementsResponse.Create(aggregatedMeasurements);
    }

    public async Task<GetAggregatedMeasurementsResponse> GetAggregatedByYearAsync(GetAggregatedByYearRequest request)
    {
        var year = new Year(request.Year);
        var aggregatedMeasurements = await measurementsRepository
            .GetAggregatedByYearAsync(request.MeteringPointId, year)
            .ToListAsync() ?? throw new MeasurementsNotFoundDuringPeriodException();

        return GetAggregatedMeasurementsResponse.Create(aggregatedMeasurements);
    }
}

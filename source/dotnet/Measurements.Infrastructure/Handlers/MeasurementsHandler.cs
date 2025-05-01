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
    public async Task<MeasurementsResponse> GetByPeriodAsync(GetByPeriodRequest request)
    {
        var foundMeasurements = await measurementsRepository
            .GetByPeriodAsync(request.MeteringPointId, request.StartDate.ToInstant(), request.EndDate.ToInstant())
            .ToListAsync() ?? throw new MeasurementsNotFoundException();

        return MeasurementsResponse.Create(foundMeasurements);
    }

    public async Task<MeasurementsAggregatedByDateResponse> GetAggregatedByDateAsync(GetAggregatedByDateRequest request)
    {
        var yearMonth = new YearMonth(request.Year, request.Month);
        var aggregatedMeasurements = await measurementsRepository
            .GetAggregatedByDateAsync(request.MeteringPointId, yearMonth)
            .ToListAsync() ?? throw new MeasurementsNotFoundException();

        return MeasurementsAggregatedByDateResponse.Create(aggregatedMeasurements);
    }

    public async Task<MeasurementsAggregatedByMonthResponse> GetAggregatedByMonthAsync(GetAggregatedByMonthRequest request)
    {
        var year = new Year(request.Year);
        var aggregatedMeasurements = await measurementsRepository
            .GetAggregatedByMonthAsync(request.MeteringPointId, year)
            .ToListAsync() ?? throw new MeasurementsNotFoundException();

        return MeasurementsAggregatedByMonthResponse.Create(aggregatedMeasurements);
    }

    public async Task<MeasurementsAggregatedByPeriodResponse> GetAggregatedByPeriodAsync(GetAggregatedByPeriodRequest getAggregatedByPeriodRequest)
    {
        var aggregatedMeasurements = await measurementsRepository
            .GetAggregatedByPeriodAsync(getAggregatedByPeriodRequest.MeteringPointIds, getAggregatedByPeriodRequest.DateFrom, getAggregatedByPeriodRequest.DateTo, getAggregatedByPeriodRequest.Aggregation)
            .ToListAsync() ?? throw new MeasurementsNotFoundDuringPeriodException();

        return MeasurementsAggregatedByPeriodResponse.Create(aggregatedMeasurements);
    }
}

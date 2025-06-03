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
    public async Task<MeasurementsResponse?> GetByPeriodAsync(GetByPeriodRequest request)
    {
        var foundMeasurements = await measurementsRepository
            .GetByPeriodAsync(request.MeteringPointId, request.StartDate.ToInstant(), request.EndDate.ToInstant())
            .ToListAsync();

        return foundMeasurements.Count <= 0 ? null : MeasurementsResponse.Create(foundMeasurements);
    }

    [Obsolete("Obsolete use GetAggregatedByDateAsync instead.")]
    public async Task<MeasurementsAggregatedByDateResponseV4?> GetAggregatedByDateAsyncV4(GetAggregatedByDateRequest request)
    {
        var yearMonth = new YearMonth(request.Year, request.Month);
        var aggregatedMeasurements = await measurementsRepository
            .GetAggregatedByDateAsync(request.MeteringPointId, yearMonth)
            .ToListAsync();

        return aggregatedMeasurements.Count <= 0 ? null : MeasurementsAggregatedByDateResponseV4.Create(aggregatedMeasurements);
    }

    public async Task<MeasurementsAggregatedByDateResponse?> GetAggregatedByDateAsync(GetAggregatedByDateRequest request)
    {
        var yearMonth = new YearMonth(request.Year, request.Month);
        var aggregatedMeasurements = await measurementsRepository
            .GetAggregatedByDateAsync(request.MeteringPointId, yearMonth)
            .ToListAsync();

        return aggregatedMeasurements.Count <= 0 ? null : MeasurementsAggregatedByDateResponse.Create(aggregatedMeasurements);
    }

    [Obsolete("Use GetAggregatedByMonthAsync instead.")]
    public async Task<MeasurementsAggregatedByMonthResponseV4?> GetAggregatedByMonthAsyncV4(GetAggregatedByMonthRequest request)
    {
        var year = new Year(request.Year);
        var aggregatedMeasurements = await measurementsRepository
            .GetAggregatedByMonthAsync(request.MeteringPointId, year)
            .ToListAsync();

        return aggregatedMeasurements.Count <= 0 ? null : MeasurementsAggregatedByMonthResponseV4.Create(aggregatedMeasurements);
    }

    public async Task<MeasurementsAggregatedByMonthResponse?> GetAggregatedByMonthAsync(GetAggregatedByMonthRequest request)
    {
        var year = new Year(request.Year);
        var aggregatedMeasurements = await measurementsRepository
            .GetAggregatedByMonthAsync(request.MeteringPointId, year)
            .ToListAsync();

        return aggregatedMeasurements.Count <= 0 ? null : MeasurementsAggregatedByMonthResponse.Create(aggregatedMeasurements);
    }

    public async Task<MeasurementsAggregatedByPeriodResponse?> GetAggregatedByPeriodAsync(GetAggregatedByPeriodRequest getAggregatedByPeriodRequest)
    {
        var aggregatedMeasurements = await measurementsRepository
            .GetAggregatedByPeriodAsync(getAggregatedByPeriodRequest.MeteringPointIds, getAggregatedByPeriodRequest.From, getAggregatedByPeriodRequest.To, getAggregatedByPeriodRequest.Aggregation)
            .ToListAsync();

        return aggregatedMeasurements.Count <= 0 ? null : MeasurementsAggregatedByPeriodResponse.Create(aggregatedMeasurements);
    }

    [Obsolete("Use GetAggregatedByYearAsync instead.")]
    public async Task<MeasurementsAggregatedByYearResponseV4?> GetAggregatedByYearAsyncV4(GetAggregatedByYearRequest request)
    {
        var aggregatedMeasurements = await measurementsRepository
            .GetAggregatedByYearAsync(request.MeteringPointId)
            .ToListAsync();

        return aggregatedMeasurements.Count <= 0 ? null : MeasurementsAggregatedByYearResponseV4.Create(aggregatedMeasurements);
    }

    public async Task<MeasurementsAggregatedByYearResponse?> GetAggregatedByYearAsync(GetAggregatedByYearRequest request)
    {
        var aggregatedMeasurements = await measurementsRepository
            .GetAggregatedByYearAsync(request.MeteringPointId)
            .ToListAsync();

        return aggregatedMeasurements.Count <= 0 ? null : MeasurementsAggregatedByYearResponse.Create(aggregatedMeasurements);
    }
}

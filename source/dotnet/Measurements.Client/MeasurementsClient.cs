using System.Collections.ObjectModel;
using System.Net;
using System.Text.Json;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.Client.ResponseParsers;
using NodaTime;

namespace Energinet.DataHub.Measurements.Client;

public class MeasurementsClient(
    IHttpClientFactory httpClientFactory,
    IMeasurementsForDayResponseParser measurementsForDayResponseParser)
    : IMeasurementsClient
{
    private readonly HttpClient _httpClient = httpClientFactory.CreateClient(MeasurementsHttpClientNames.MeasurementsApi);

    public async Task<MeasurementDto> GetByDayAsync(GetByDayQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateGetMeasurementsForPeriodUrl(query.MeteringPointId, query.Date, query.Date.PlusDays(1));

        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        if (response.StatusCode == HttpStatusCode.NotFound)
            return new MeasurementDto([]);

        var result = await measurementsForDayResponseParser.ParseResponseMessage(response, cancellationToken);

        return result ?? throw new InvalidOperationException("The response was not successfully parsed.");
    }

    public Task<ReadOnlyCollection<MeasurementPointDto>> GetByPeriodAsync(GetByPeriodQuery query, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public async Task<IEnumerable<MeasurementAggregationByDateDto>> GetMonthlyAggregateByDateAsync(
        GetMonthlyAggregateByDateQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateGetMeasurementsAggregatedByDateUrl(query.MeteringPointId, query.YearMonth);

        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        return await ParseMeasurementAggregationResponseAsync<MeasurementAggregationByDateDto>(response, cancellationToken);
    }

    public async Task<IEnumerable<MeasurementAggregationByMonthDto>> GetYearlyAggregateByMonthAsync(
        GetYearlyAggregateByMonthQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateGetMeasurementsAggregatedByMonthUrl(query.MeteringPointId, query.Year);

        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        return await ParseMeasurementAggregationResponseAsync<MeasurementAggregationByMonthDto>(response, cancellationToken);
    }

    public async Task<IEnumerable<MeasurementAggregationByYearDto>> GetAggregateByYearAsync(GetAggregateByYearQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateGetMeasurementsAggregatedByYearUrl(query.MeteringPointId);

        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        return await ParseMeasurementAggregationResponseAsync<MeasurementAggregationByYearDto>(response, cancellationToken);
    }

    public async Task<IEnumerable<MeasurementAggregationByPeriodDto>> GetAggregateByPeriodAsync(GetAggregateByPeriodQuery query, CancellationToken cancellationToken = default)
    {
        var meteringPointIdsString = string.Join(",", query.MeteringPointIds);
        var url = CreateGetMeasurementsAggregatedByPeriodUrl(meteringPointIdsString, query.From, query.To, query.Aggregation);

        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        return response.StatusCode == HttpStatusCode.Accepted
            ? []
            : throw new HttpRequestException($"Request failed with status code: {response.StatusCode}");
    }

    private async Task<IEnumerable<T>> ParseMeasurementAggregationResponseAsync<T>(
        HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.StatusCode == HttpStatusCode.NotFound)
            return [];

        var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        return await DeserializeMeasurementAggregationResponseStreamAsync<T>(stream, cancellationToken);
    }

    private async Task<IEnumerable<T>> DeserializeMeasurementAggregationResponseStreamAsync<T>(
        Stream stream, CancellationToken cancellationToken)
    {
        var jsonDocument = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        var pointElement = jsonDocument.RootElement.GetProperty("MeasurementAggregations");
        var options = new Serialization.JsonSerializer().Options;

        return pointElement.Deserialize<IEnumerable<T>>(options) ?? throw new InvalidOperationException();
    }

    private static string CreateGetMeasurementsForPeriodUrl(string meteringPointId, LocalDate fromDate, LocalDate toDate)
    {
        return $"v2/measurements/forPeriod?MeteringPointId={meteringPointId}&StartDate={fromDate.ToUtcString()}&EndDate={toDate.ToUtcString()}";
    }

    private static string CreateGetMeasurementsAggregatedByDateUrl(string meteringPointId, YearMonth yearMonth)
    {
        return $"v3/measurements/aggregatedByDate?MeteringPointId={meteringPointId}&Year={yearMonth.Year}&Month={yearMonth.Month}";
    }

    private static string CreateGetMeasurementsAggregatedByMonthUrl(string meteringPointId, int year)
    {
        return $"v3/measurements/aggregatedByMonth?MeteringPointId={meteringPointId}&Year={year}";
    }

    private static string CreateGetMeasurementsAggregatedByYearUrl(string meteringPointId)
    {
        return $"v3/measurements/aggregatedByYear?MeteringPointId={meteringPointId}";
    }

    private static string CreateGetMeasurementsAggregatedByPeriodUrl(string meteringPointIds, Instant from, Instant to, Aggregation aggregation)
    {
        return $"v3/measurements/aggregatedByPeriod?MeteringPointIds={meteringPointIds}&From={from}&To={to}&Aggregation={aggregation}";
    }
}

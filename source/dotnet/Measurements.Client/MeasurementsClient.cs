using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.Client.Factories;
using Energinet.DataHub.Measurements.Client.Models;
using NodaTime;

namespace Energinet.DataHub.Measurements.Client;

public class MeasurementsClient : IMeasurementsClient
{
    private readonly HttpClient _httpClient;

    private readonly JsonSerializerOptions _jsonSerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() },
    };

    private readonly MeasurementAggregationFactory _measurementAggregationFactory;

    public MeasurementsClient(IHttpClientFactory httpClientFactory, MeasurementAggregationFactory measurementAggregationFactory)
    {
        _measurementAggregationFactory = measurementAggregationFactory;
        _httpClient = httpClientFactory.CreateClient(MeasurementsHttpClientNames.MeasurementsApi);
    }

    public async Task<IEnumerable<MeasurementPointDto>> GetMeasurementsForDayAsync(
        GetMeasurementsForDayQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateUrl(query.MeteringPointId, query.Date, query.Date.PlusDays(1));

        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        return await ParseMeasurementsResponseAsync(response, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IEnumerable<MeasurementPointDto>> GetMeasurementsForPeriodAsync(
        GetMeasurementsForPeriodQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateUrl(query.MeteringPointId, query.FromDate, query.ToDate);

        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        return await ParseMeasurementsResponseAsync(response, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IEnumerable<MeasurementAggregation>> GetAggregatedMeasurementsForMonth(
        GetAggregatedMeasurementsForMonthQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateUrl(query.MeteringPointId, query.YearMonth);

        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        return await ParseMeasurementAggregationResponseAsync(query.YearMonth, response, cancellationToken);
    }

    private async Task<IEnumerable<MeasurementPointDto>> ParseMeasurementsResponseAsync(
        HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.StatusCode == HttpStatusCode.NotFound) return [];

        var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        var measurementPoints = await DeserializeMeasurementsResponseStreamAsync(stream, cancellationToken);

        return measurementPoints;
    }

    private async Task<IEnumerable<MeasurementPointDto>> DeserializeMeasurementsResponseStreamAsync(
        Stream stream, CancellationToken cancellationToken)
    {
        var jsonDocument = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        var pointElement = jsonDocument.RootElement.GetProperty("Points");

        return pointElement.Deserialize<IEnumerable<MeasurementPointDto>>(_jsonSerializerOptions)
               ?? throw new InvalidOperationException();
    }

    private async Task<IEnumerable<MeasurementAggregation>> ParseMeasurementAggregationResponseAsync(
        YearMonth yearMonth, HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.StatusCode == HttpStatusCode.NotFound) return [];

        var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        var aggregatedMeasurements = await DeserializeMeasurementAggregationResponseStreamAsync(stream, cancellationToken);

        return aggregatedMeasurements.Select(am => _measurementAggregationFactory.Create(am)).ToList();

        //return aggregatedMeasurementsDictionary.Select(am => _measurementAggregationFactory.Create(am.Value, am.Key));
    }

    private async Task<IEnumerable<AggregatedMeasurements>> DeserializeMeasurementAggregationResponseStreamAsync(
        Stream stream, CancellationToken cancellationToken)
    {
        var jsonDocument = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        var pointElement = jsonDocument.RootElement.GetProperty("MeasurementAggregations");

        return pointElement.Deserialize<IEnumerable<AggregatedMeasurements>>(_jsonSerializerOptions)
               ?? throw new InvalidOperationException();
    }

    private static string CreateUrl(string meteringPointId, LocalDate fromDate, LocalDate toDate)
    {
        return $"/measurements/forPeriod?MeteringPointId={meteringPointId}&StartDate={fromDate.ToUtcString()}&EndDate={toDate.ToUtcString()}";
    }

    private static string CreateUrl(string meteringPointId, YearMonth yearMonth)
    {
        return $"/measurements/aggregatedByMonth?MeteringPointId={meteringPointId}&Year={yearMonth.Year}&Month={yearMonth.Month}";
    }
}

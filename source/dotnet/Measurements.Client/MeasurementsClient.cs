using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
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

    public MeasurementsClient(IHttpClientFactory httpClientFactory)
    {
        _httpClient = httpClientFactory.CreateClient(MeasurementsHttpClientNames.MeasurementsApi);
    }

    public async Task<IEnumerable<MeasurementPoint>> GetMeasurementsForDayAsync(
        GetMeasurementsForDayQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateUrl(query.MeteringPointId, query.Date, query.Date.PlusDays(1));

        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        return await ParseMeasurementsResponseAsync(response, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IEnumerable<MeasurementPoint>> GetMeasurementsForPeriodAsync(
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

        return await ParseMeasurementAggregationResponseAsync(response, cancellationToken);
    }

    private async Task<IEnumerable<MeasurementPoint>> ParseMeasurementsResponseAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.StatusCode == HttpStatusCode.NotFound) return [];

        var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        var measurementPoints = await DeserializeMeasurementsResponseStreamAsync(stream, cancellationToken);

        return measurementPoints;
    }

    private async Task<IEnumerable<MeasurementPoint>> DeserializeMeasurementsResponseStreamAsync(Stream stream, CancellationToken cancellationToken)
    {
        var jsonDocument = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        var pointElement = jsonDocument.RootElement.GetProperty("Points");

        return pointElement.Deserialize<IEnumerable<MeasurementPoint>>(_jsonSerializerOptions) ?? throw new InvalidOperationException();
    }

    private async Task<IEnumerable<MeasurementAggregation>> ParseMeasurementAggregationResponseAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.StatusCode == HttpStatusCode.NotFound) return [];

        var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        var measurementAggregation = await DeserializeMeasurementAggregationResponseStreamAsync(stream, cancellationToken);

        IEnumerable<MeasurementAggregation> measurementAggregations = [];

        foreach (var measurementPoint in measurementAggregation)
        {
            var measurementAggregationPoint = new MeasurementAggregation(
                measurementPoint.MinObservationTime.ToLocalDate(),
                measurementPoint.Quantity,
                SetMissingValuesForAggregation(measurementPoint.PointCount, measurementPoint.MinObservationTime, measurementPoint.MaxObservationTime));

            measurementAggregations = measurementAggregations.Append(measurementAggregationPoint);
        }

        return measurementAggregations;
    }

    private bool SetMissingValuesForAggregation(int pointCount, DateTimeOffset minObservationTime, DateTimeOffset maxObservationTime)
    {
        var timeSpan = maxObservationTime - minObservationTime;
        var hours = timeSpan.TotalHours + 1;
        return Math.Abs(hours - pointCount) == 0;
    }

    private async Task<IEnumerable<AggregatedMeasurements>> DeserializeMeasurementAggregationResponseStreamAsync(Stream stream, CancellationToken cancellationToken)
    {
        var jsonDocument = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        var pointElement = jsonDocument.RootElement.GetProperty("MeasurementAggregations");

        return pointElement.Deserialize<IEnumerable<AggregatedMeasurements>>(_jsonSerializerOptions) ?? throw new InvalidOperationException();
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

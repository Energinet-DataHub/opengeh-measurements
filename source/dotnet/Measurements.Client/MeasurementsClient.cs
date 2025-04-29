using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
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

    private readonly JsonSerializerOptions _jsonSerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() },
    };

    public async Task<MeasurementDto> GetByDayAsync(GetByDayQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateGetMeasurementsForPeriodUrl(query.MeteringPointId, query.Date, query.Date.PlusDays(1));

        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        if (response.StatusCode == HttpStatusCode.NotFound)
            return new MeasurementDto([]);

        var result = await measurementsForDayResponseParser.ParseResponseMessage(response, cancellationToken);

        return result ?? throw new InvalidOperationException("The response was not successfully parsed.");
    }

    public async Task<IEnumerable<MeasurementAggregationDto>> GetAggregatedByMonth(
        GetAggregatedByMonthQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateGetMeasurementsAggregatedByMonthUrl(query.MeteringPointId, query.YearMonth);

        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        return await ParseMeasurementAggregationResponseAsync(response, cancellationToken);
    }

    private async Task<IEnumerable<MeasurementAggregationDto>> ParseMeasurementAggregationResponseAsync(
        HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.StatusCode == HttpStatusCode.NotFound)
            return [];

        var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        return await DeserializeMeasurementAggregationResponseStreamAsync(stream, cancellationToken);
    }

    private async Task<IEnumerable<MeasurementAggregationDto>> DeserializeMeasurementAggregationResponseStreamAsync(
        Stream stream, CancellationToken cancellationToken)
    {
        var jsonDocument = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        var pointElement = jsonDocument.RootElement.GetProperty("MeasurementAggregations");

        return pointElement.Deserialize<IEnumerable<MeasurementAggregationDto>>(_jsonSerializerOptions)
               ?? throw new InvalidOperationException();
    }

    private static string CreateGetMeasurementsForPeriodUrl(string meteringPointId, LocalDate fromDate, LocalDate toDate)
    {
        return $"v2/measurements/forPeriod?MeteringPointId={meteringPointId}&StartDate={fromDate.ToUtcString()}&EndDate={toDate.ToUtcString()}";
    }

    private static string CreateGetMeasurementsAggregatedByMonthUrl(string meteringPointId, YearMonth yearMonth)
    {
        return $"v2/measurements/aggregatedByMonth?MeteringPointId={meteringPointId}&Year={yearMonth.Year}&Month={yearMonth.Month}";
    }
}

using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
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

    public async Task<IEnumerable<MeasurementPoint>> GetMeasurementsForDayAsync(GetMeasurementsForDayQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateUrl(query.MeteringPointId, query.Date, query.Date.PlusDays(1));

        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        return await ParseResponseAsync(response, cancellationToken).ConfigureAwait(false);
    }

    public async Task<IEnumerable<MeasurementPoint>> GetMeasurementsForPeriodAsync(GetMeasurementsForPeriodQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateUrl(query.MeteringPointId, query.FromDate, query.ToDate);

        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        return await ParseResponseAsync(response, cancellationToken).ConfigureAwait(false);
    }

    private async Task<IEnumerable<MeasurementPoint>> ParseResponseAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.StatusCode == HttpStatusCode.NotFound) return [];

        var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        var measurementPoints = await DeserializeResponseStreamAsync(stream, cancellationToken);

        return measurementPoints;
    }

    private async Task<IEnumerable<MeasurementPoint>> DeserializeResponseStreamAsync(Stream stream, CancellationToken cancellationToken)
    {
        var jsonDocument = await JsonDocument.ParseAsync(stream, cancellationToken: cancellationToken);
        var pointElement = jsonDocument.RootElement.GetProperty("Points");

        return pointElement.Deserialize<IEnumerable<MeasurementPoint>>(_jsonSerializerOptions) ?? throw new InvalidOperationException();
    }

    private static string CreateUrl(string meteringPointId, LocalDate fromDate, LocalDate toDate)
    {
        return $"/measurements?MeteringPointId={meteringPointId}&StartDate={fromDate.ToUtcString()}&EndDate={toDate.ToUtcString()}";
    }
}

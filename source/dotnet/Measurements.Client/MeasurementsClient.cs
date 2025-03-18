using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;

namespace Energinet.DataHub.Measurements.Client;

public class MeasurementsClient(IHttpClientFactory httpClientFactory) : IMeasurementsClient
{
    private readonly HttpClient _httpClient = httpClientFactory.CreateClient(MeasurementsHttpClientNames.MeasurementsApi);

    private readonly JsonSerializerOptions _jsonSerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() },
    };

    public async Task<IEnumerable<MeasurementPoint>> GetMeasurementsForDayAsync(GetMeasurementsForDayQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateUrl(
            query.MeteringPointId,
            query.Date.ToUtcString(),
            query.Date.PlusDays(1).ToUtcString());

        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        return await HandleResponseAsync(response, cancellationToken);
    }

    public async Task<IEnumerable<MeasurementPoint>> GetMeasurementsForPeriodAsync(GetMeasurementsForPeriodQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateUrl(query.MeteringPointId, query.FromDate.ToUtcString(), query.ToDate.ToUtcString());

        var response = await _httpClient.GetAsync(url, cancellationToken);

        return await HandleResponseAsync(response, cancellationToken);
    }

    private async Task<IEnumerable<MeasurementPoint>> HandleResponseAsync(HttpResponseMessage response, CancellationToken cancellationToken)
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

    private static string CreateUrl(string meteringPointId, string startDate, string endDate)
    {
        return $"/measurements?MeteringPointId={meteringPointId}&StartDate={startDate}&EndDate={endDate}";
    }
}

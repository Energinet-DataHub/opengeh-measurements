using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Converter;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
using NodaTime.Serialization.SystemTextJson;

namespace Energinet.DataHub.Measurements.Client;

public class MeasurementsClient(IHttpClientFactory httpClientFactory) : IMeasurementsClient
{
    private readonly HttpClient _httpClient = httpClientFactory.CreateClient(MeasurementsHttpClientNames.MeasurementsApi);

    private readonly JsonSerializerOptions _jsonSerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters =
        {
            NodaConverters.InstantConverter,
            new JsonStringEnumConverter(),
            new MeasurementPointConverter(),
        },
    };

    public async Task<IEnumerable<MeasurementPoint>> GetMeasurementsForDayAsync(GetMeasurementsForDayQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateUrl(query);
        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        if (response.StatusCode == HttpStatusCode.NotFound) return [];

        var measurementPoints = await response.Content
            .ReadFromJsonAsync<IEnumerable<MeasurementPoint>>(_jsonSerializerOptions, cancellationToken)
            .ConfigureAwait(false);

        return measurementPoints ?? throw new InvalidOperationException("Could not deserialize response.");
    }

    private static string CreateUrl(GetMeasurementsForDayQuery query)
    {
        var startDate = query.Date.ToUtcString();
        var endDate = query.Date.PlusDays(1).ToUtcString();

        return $"/measurements?MeteringPointId={query.MeteringPointId}&StartDate={startDate}&EndDate={endDate}";
    }
}

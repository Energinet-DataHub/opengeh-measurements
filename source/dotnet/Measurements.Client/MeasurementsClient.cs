using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;

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

    public async Task<MeasurementDto> GetMeasurementsForDayAsync(GetMeasurementsForDayQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateUrl(query);
        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return new MeasurementDto([]);
        }

        var measurement = await response.Content
            .ReadFromJsonAsync<MeasurementDto>(_jsonSerializerOptions, cancellationToken)
            .ConfigureAwait(false);

        return measurement ?? throw new InvalidOperationException("Could not deserialize response.");
    }

    private static string CreateUrl(GetMeasurementsForDayQuery query)
    {
        var startDate = query.Date.ToUtcString();
        var endDate = query.Date.PlusDays(1).ToUtcString();

        return $"/measurements?MeteringPointId={query.MeteringPointId}&StartDate={startDate}&EndDate={endDate}";
    }
}

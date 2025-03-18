using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
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

    public async Task<MeasurementDto?> GetMeasurementsForDayAsync(GetMeasurementsForDayQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateUrl(query);
        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        response.EnsureSuccessStatusCode();

        var content = await response.Content.ReadAsStringAsync(cancellationToken);
        return JsonSerializer.Deserialize<MeasurementDto>(content, _jsonSerializerOptions);
    }

    private static string CreateUrl(GetMeasurementsForDayQuery query)
    {
        return $"/measurements?MeteringPointId={query.MeteringPointId}&StartDate={FormatDate(query.Date)}&EndDate={FormatDate(query.Date.AddDays(1))}";
    }

    private static string FormatDate(DateTimeOffset date)
    {
        return date.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
    }
}

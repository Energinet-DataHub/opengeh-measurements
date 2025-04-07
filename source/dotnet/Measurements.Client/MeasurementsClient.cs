using System.Net;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.Client.Serialization;
using NodaTime;

namespace Energinet.DataHub.Measurements.Client;

public class MeasurementsClient : IMeasurementsClient
{
    private readonly HttpClient _httpClient;

    public MeasurementsClient(IHttpClientFactory httpClientFactory)
    {
        _httpClient = httpClientFactory.CreateClient(MeasurementsHttpClientNames.MeasurementsApi);
    }

    public async Task<MeasurementDto> GetMeasurementsForDayAsync(GetMeasurementsForDayQuery query, CancellationToken cancellationToken = default)
    {
        var url = CreateUrl(query.MeteringPointId, query.Date, query.Date.PlusDays(1));

        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        return await ParseResponseAsync(response, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<MeasurementDto> ParseResponseAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.StatusCode == HttpStatusCode.NotFound) return new MeasurementDto([]);

        var json = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        var result = new MeasurementSerializer().Deserialize<MeasurementDto>(json);

        return result ?? throw new InvalidOperationException("The response was not successfully parsed.");
    }

    private static string CreateUrl(string meteringPointId, LocalDate fromDate, LocalDate toDate)
    {
        return $"/measurements?MeteringPointId={meteringPointId}&StartDate={fromDate.ToUtcString()}&EndDate={toDate.ToUtcString()}";
    }
}

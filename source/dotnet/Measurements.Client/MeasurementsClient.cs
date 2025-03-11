using System.Net.Http.Json;
using Energinet.DataHub.Measurements.Abstractions.Api.Dtos;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
using NodaTime;

namespace Energinet.DataHub.Measurements.Client;

public class MeasurementsClient : IMeasurementsClient
{
    private readonly HttpClient _httpClient;

    public MeasurementsClient(IHttpClientFactory httpClientFactory)
    {
        _httpClient = httpClientFactory.CreateClient(MeasurementsHttpClientNames.MeasurementsApi);
    }

    public async Task<MeasurementDto?> GetMeasurementsForDayAsync(GetMeasurementsForDayQuery query, CancellationToken cancellationToken = default)
    {
        var response = await _httpClient
            .GetAsync(
                $"/measurements?MeteringPointId={query.MeteringPointId}&StartDate={query.Date}&EndDate={query.Date.Plus(Duration.FromDays(1))}",
                cancellationToken)
            .ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
        {
            return null;
        }

        var measurementDto = await response.Content
            .ReadFromJsonAsync<MeasurementDto>(cancellationToken)
            .ConfigureAwait(false);

        return measurementDto;
    }
}

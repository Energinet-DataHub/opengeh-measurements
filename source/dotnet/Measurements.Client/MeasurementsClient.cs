using System.Net.Http.Json;
using Energinet.DataHub.Measurements.Abstractions.Api.Dtos;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;

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
                $"/measurements?MeteringPointId={query.MeteringPointId}&StartDate={query.Date}&EndDate={query.Date.AddDays(1)}",
                cancellationToken)
            .ConfigureAwait(false);

        response.EnsureSuccessStatusCode();

        var measurementDto = await response.Content
            .ReadFromJsonAsync<MeasurementDto>(cancellationToken)
            .ConfigureAwait(false);

        return measurementDto;
    }
}

using System.Net.Http.Json;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
using Measurements.Abstractions.Api.Dtos;
using Measurements.Abstractions.Api.Queries;
using NodaTime;

namespace Energinet.DataHub.Measurements.Client;

public class MeasurementsClient : IMeasurementsClient
{
    private readonly HttpClient _measurementsApiClient;

    public MeasurementsClient(IHttpClientFactory httpClientFactory)
    {
        _measurementsApiClient = httpClientFactory.CreateClient(MeasurementsHttpClientNames.MeasurementsApi);
    }

    public async Task<MeasurementDto?> GetMeasurementAsync(GetMeasurementsForDayQuery query, CancellationToken cancellationToken = default)
    {
        var response = await _measurementsApiClient
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

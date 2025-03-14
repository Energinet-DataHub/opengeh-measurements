﻿using System.Globalization;
using System.Net.Http.Json;
using Energinet.DataHub.Measurements.Abstractions.Api.Dtos;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;

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
        var url = CreateUrl(query);
        var response = await _httpClient.GetAsync(url, cancellationToken).ConfigureAwait(false);

        response.EnsureSuccessStatusCode();

        // var measurementDto = await response.Content
        //     .ReadFromJsonAsync<MeasurementDto>(cancellationToken)
        //     .ConfigureAwait(false);
        var content = await response.Content.ReadAsStringAsync(cancellationToken);
        var measDto = new JsonSerializer().Deserialize<MeasurementDto>(content);

        return measDto;
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

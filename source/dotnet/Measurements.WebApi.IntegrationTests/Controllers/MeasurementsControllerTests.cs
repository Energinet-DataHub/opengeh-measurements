﻿using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.WebApi.IntegrationTests.Extensions;
using Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;
using Xunit;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Controllers;

public class MeasurementsControllerTests(WebApiFixture fixture)
    : IClassFixture<WebApiFixture>
{
    private readonly HttpClient _client = fixture.CreateClient();

    [Fact]
    public async Task GetAsync_ReturnsValidMeasurement()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        const string startDate = "2022-01-01T00:00:00Z";
        const string endDate = "2022-01-02T00:00:00Z";
        var url = CreateUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actualResponse = await _client.GetAsync(url);
        var actual = await ParseResponseAsync(actualResponse);

        // Assert
        Assert.Equal(expectedMeteringPointId, actual.MeteringPointId);
        Assert.Equal(Unit.KWh, actual.Unit);
        Assert.Equal(24, actual.Points.Count);
        Assert.True(actual.Points.All(p => p.ObservationTime.ToString() == startDate));
        Assert.True(actual.Points.All(p => p.Quality == Quality.Measured));
    }

    private static string CreateUrl(string expectedMeteringPointId, string startDate, string endDate)
    {
        return $"measurements?meteringPointId={expectedMeteringPointId}&startDate={startDate}&endDate={endDate}";
    }

    private async Task<GetMeasurementResponse> ParseResponseAsync(HttpResponseMessage response)
    {
        response.EnsureSuccessStatusCode();
        var actualBody = await response.Content.ReadAsStringAsync();
        return actualBody.DeserializeJson<GetMeasurementResponse>();
    }
}

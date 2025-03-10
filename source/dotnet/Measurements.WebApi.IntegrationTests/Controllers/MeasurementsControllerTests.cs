using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;
using NodaTime;
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
        var url = CreateUrl(expectedMeteringPointId);

        // Act
        var actualResponse = await _client.GetAsync(url);
        var actual = await ParseResponseAsync(actualResponse);

        // Assert
        Assert.Equal(expectedMeteringPointId, actual.MeteringPointId);
        Assert.Equal(Unit.kWh, actual.Unit);
        Assert.Equal(24, actual.Points.Count);
        Assert.True(actual.Points.All(p => p.Quality == Quality.Measured));
    }

    private static string CreateUrl(string expectedMeteringPointId)
    {
        var startDate = Instant.FromUtc(2022, 1, 1, 0, 0);
        var endDate = Instant.FromUtc(2022, 1, 2, 0, 0);
        return $"measurements?meteringPointId={expectedMeteringPointId}&startDate={startDate}&endDate={endDate}";
    }

    private async Task<GetMeasurementResponse> ParseResponseAsync(HttpResponseMessage response)
    {
        response.EnsureSuccessStatusCode();
        var actualBody = await response.Content.ReadAsStringAsync();

        return new JsonSerializer().Deserialize<GetMeasurementResponse>(actualBody);
    }
}

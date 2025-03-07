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
        actualResponse.EnsureSuccessStatusCode();
        var actualBody = await actualResponse.Content.ReadAsStringAsync();
        var actualContentType = actualResponse.Content.Headers.ContentType!.MediaType!;
        // var actualMeasurementResponse = new JsonSerializer().Deserialize<GetMeasurementResponse>(actualBody);

        // Assert
        Assert.Equal("application/json", actualContentType);
        Assert.Equal(ExpectedBody(), actualBody);

        // Assert.Equal(expectedMeteringPointId, actualMeasurementResponse.MeteringPointId);
        // Assert.Equal(24, actualMeasurementResponse.Points.Count());
        // Assert.True(actualMeasurementResponse.Points.All(p => p.Quality == Quality.Measured));
    }

    private string CreateUrl(string expectedMeteringPointId)
    {
        var startDate = Instant.FromUtc(2022, 1, 1, 0, 0);
        var endDate = Instant.FromUtc(2022, 1, 2, 0, 0);
        return $"measurements?meteringPointId={expectedMeteringPointId}&startDate={startDate}&endDate={endDate}";
    }

    private static string ExpectedBody() =>
        "{\"meteringPointId\":\"1234567890\",\"unit\":0,\"points\":[{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1},{\"observationTime\":\"2022-01-01T00:00:00+00:00\",\"quantity\":1,\"quality\":1}]}";
}

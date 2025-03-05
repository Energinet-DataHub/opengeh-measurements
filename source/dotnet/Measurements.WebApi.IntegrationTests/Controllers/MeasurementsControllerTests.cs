using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;
using NodaTime;
using Xunit;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Controllers;

public class MeasurementsControllerTests(WebApiFixture fixture)
    : IClassFixture<WebApiFixture>
{
    [Fact]
    public async Task GetAsync_ReturnsFortyTwo()
    {
        // Arrange
        const string meteringPointId = "1234567890";
        var startDate = Instant.FromUtc(2022, 1, 1, 0, 0);
        var endDate = Instant.FromUtc(2022, 1, 2, 0, 0);
        var client = fixture.CreateClient();
        var url = $"measurements?meteringPointId={meteringPointId}&startDate={startDate}&endDate={endDate}";

        // Act
        var response = await client.GetAsync(url);
        response.EnsureSuccessStatusCode();
        var responseBody = await response.Content.ReadAsStringAsync();
        var measurementResponse = new JsonSerializer().Deserialize<GetMeasurementResponse>(responseBody);

        // Assert
        Assert.Equal(meteringPointId, measurementResponse.MeteringPointId);
        Assert.Equal(42, measurementResponse.Points.First().Quantity);
    }
}

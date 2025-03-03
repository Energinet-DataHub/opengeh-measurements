using Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;
using Xunit;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Controllers;

public class MeasurementsControllerTests(WebApiFixture fixture)
    : IClassFixture<WebApiFixture>
{
    [Theory]
    [InlineData("meteringPointId", "42")]
    public async Task GetAsync_ReturnsFortyTwo(
        string meteringPointId,
        string expectedResponseBody)
    {
        // Arrange
        var client = fixture.CreateClient();
        var url = $"measurements?measurementId={meteringPointId}";

        // Act
        var response = await client.GetAsync(url);
        response.EnsureSuccessStatusCode();
        var actualResponseBody = await response.Content.ReadAsStringAsync();

        // Assert
        Assert.Equal(expectedResponseBody, actualResponseBody);
    }
}

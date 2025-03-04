using Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Controllers;

[IntegrationTest]
[Collection(nameof(WebApiCollectionFixture))]
public class HealthCheckTests(WebApiFixture fixture) : IClassFixture<WebApiFixture>
{
    [Theory]
    [InlineData("/monitor/status")]
    [InlineData("/monitor/live")]
    [InlineData("/monitor/ready")]
    public async Task Get_EndpointsReturnSuccessAndCorrectContentType(string url)
    {
        // Arrange
        var client = fixture.CreateClient();

        // Act
        var response = await client.GetAsync(url);
        client.Dispose();

        // Assert
        response.EnsureSuccessStatusCode(); // Status Code 200-299
        Assert.Equal("application/json", response.Content.Headers.ContentType!.ToString());
    }
}

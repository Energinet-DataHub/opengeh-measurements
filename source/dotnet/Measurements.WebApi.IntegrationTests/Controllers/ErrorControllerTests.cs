using System.Net;
using Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Controllers;

[IntegrationTest]
public class ErrorControllerTests(WebApiFixture fixture) : IClassFixture<WebApiFixture>
{
    [Fact]
    public async Task HandleError_WhenEndpointCalledDirectly_ShouldReturnBadRequest()
    {
        // Arrange
        const string url = "/error";

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.BadRequest, actualResponse.StatusCode);
    }
}

using Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Controllers;

[IntegrationTest]
public class ErrorControllerTests(WebApiFixture fixture) : IClassFixture<WebApiFixture>
{
    [Fact]
    public async Task HandleError_WhenUnhandledException_ThenRedirectToErrorController()
    {
        // Arrange

        // Act
        var actual = await fixture.Client.GetAsync("/invalid-endpoint");
        var actualContent = await actual.Content.ReadAsStringAsync();

        // Assert
        Assert.Contains("An unknown error has occured", actualContent);
    }
}

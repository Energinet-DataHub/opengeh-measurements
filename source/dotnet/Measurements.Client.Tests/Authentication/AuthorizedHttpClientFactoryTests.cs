using Energinet.DataHub.Measurements.Client.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Moq;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.Client.Tests.Authentication;

[UnitTest]
public class AuthorizedHttpClientFactoryTests
{
    [Fact]
    public void CreateClient_WhenCalled_ReturnsAuthorizedHttpClient()
    {
        // Arrange
        var httpClientFactoryMock = new Mock<IHttpClientFactory>();
        httpClientFactoryMock
            .Setup(x => x.CreateClient(It.IsAny<string>()))
            .Returns(new HttpClient());

        var authorizationHeaderProviderMock = new Mock<Func<string>>();
        authorizationHeaderProviderMock
            .Setup(x => x())
            .Returns("Bearer test-token");

        var sut = new AuthorizedHttpClientFactory(
            httpClientFactoryMock.Object, authorizationHeaderProviderMock.Object);

        // Act
        var actual = sut.CreateClient();

        // Assert
        Assert.NotNull(actual);
        Assert.NotNull(actual.DefaultRequestHeaders.Authorization);
        Assert.Equal(JwtBearerDefaults.AuthenticationScheme, actual.DefaultRequestHeaders.Authorization!.Scheme);
        Assert.Equal("test-token", actual.DefaultRequestHeaders.Authorization!.Parameter);
    }
}

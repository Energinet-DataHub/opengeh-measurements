using Azure.Core;
using Energinet.DataHub.Measurements.Client.Authentication;
using Moq;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.Client.UnitTests;

[UnitTest]
public class AuthorizationProviderTests
{
    [Fact]
    public void CreateAuthorizationHeader_WhenCalled_ThenReturnsValidAuthorizationHeader()
    {
        // Arrange
        var credential = new Mock<TokenCredential>();
        credential.Setup(tc => tc
            .GetToken(It.IsAny<TokenRequestContext>(), It.IsAny<CancellationToken>()))
            .Returns(new AccessToken("test", DateTimeOffset.UtcNow.AddHours(1)));
        var provider = new AuthorizationHeaderProvider(credential.Object, "https://example.com/applicationIdUri");

        // Act
        var actual = provider.CreateAuthenticationHeaderValue();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal("Bearer", actual.Scheme);
        Assert.Equal("test", actual.Parameter);
    }

    [Fact]
    public async Task CreateAuthorizationHeaderAsync_WhenCalled_ThenReturnsValidAuthorizationHeader()
    {
        // Arrange
        var credential = new Mock<TokenCredential>();
        credential.Setup(tc => tc
                .GetTokenAsync(It.IsAny<TokenRequestContext>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new AccessToken("test", DateTimeOffset.UtcNow.AddHours(1)));
        var provider = new AuthorizationHeaderProvider(credential.Object, "https://example.com/applicationIdUri");

        // Act
        var actual = await provider.CreateAuthenticationHeaderValueAsync();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal("Bearer", actual.Scheme);
        Assert.Equal("test", actual.Parameter);
    }
}

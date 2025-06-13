using System.Net.Http.Headers;
using Energinet.DataHub.Core.App.Common.Extensions.DependencyInjection;
using Energinet.DataHub.Core.App.Common.Identity;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.Client.Extensions.Options;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Moq;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.Client.UnitTests.Extensions;

[UnitTest]
public class ClientExtensionsTests
{
    [Fact]
    public void AddMeasurementsClient_WhenOptionsAreConfiguredAndTokenCredentialIsRegistered_ThenClientsCanBeCreated()
    {
        // Arrange
        var services = new ServiceCollection();
        var configurations = new Dictionary<string, string?>
        {
            [$"{MeasurementHttpClientOptions.SectionName}:{nameof(MeasurementHttpClientOptions.BaseAddress)}"] = "https://localhost",
            [$"{MeasurementHttpClientOptions.SectionName}:{nameof(MeasurementHttpClientOptions.ApplicationIdUri)}"] = "https://management.azure.com",
        };
        var mockedAuthorizationHeaderProvider = new Mock<IAuthorizationHeaderProvider>();
        services.AddSingleton(mockedAuthorizationHeaderProvider.Object);
        AddInMemoryConfiguration(services, configurations);

        // Act
        services.AddMeasurementsClient();

        // Assert
        var actual = services
            .BuildServiceProvider()
            .GetRequiredService<IMeasurementsClient>();
        Assert.IsType<MeasurementsClient>(actual);
    }

    [Fact]
    public void AddMeasurementsClient_WhenOptionsAreNotConfigured_ThenExceptionIsThrownWhenRequestingClient()
    {
        // Arrange
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection([])
            .Build();

        services.AddScoped<IConfiguration>(_ => configuration);

        // Act
        services.AddMeasurementsClient();

        // Assert
        var serviceProvider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(() => serviceProvider.GetRequiredService<IMeasurementsClient>());
        Assert.Contains(
            "DataAnnotation validation failed for 'MeasurementHttpClientOptions'",
            exception.Message);
    }

    [Fact]
    public async Task AddMeasurementsClient_WhenOptionsAreConfiguredAndB2CAuthorizationHeaderProviderIsRegistered_ThenClientsCanBeCreated()
    {
        // Arrange
        var services = new ServiceCollection();
        var serviceProvider = services.BuildServiceProvider();
        var expectedAuthenticationHeaderValueParameter = "Bearer some-token";
        AuthenticationHeaderValue? actualAuthenticationHeaderValue = null;
        var configurations = new Dictionary<string, string?>
        {
            [$"{MeasurementHttpClientOptions.SectionName}:{nameof(MeasurementHttpClientOptions.BaseAddress)}"] = "https://localhost",
            [$"{MeasurementHttpClientOptions.SectionName}:{nameof(MeasurementHttpClientOptions.ApplicationIdUri)}"] = "https://management.azure.com",
        };
        AddInMemoryConfiguration(services, configurations);

        // Act
        services.AddMeasurementsClient(sp =>
        {
            var authenticationHeaderValue = new AuthenticationHeaderValue("Bearer", "some-token");
            actualAuthenticationHeaderValue = authenticationHeaderValue;
            return new CustomAuthorizationHandler(authenticationHeaderValue);
        });

        // Assert
        var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
        var actual = httpClientFactory.CreateClient(MeasurementsHttpClientNames.MeasurementsApi);
        await Assert.ThrowsAsync<HttpRequestException>(() => actual.GetAsync("https://localhost"));
        Assert.NotNull(actualAuthenticationHeaderValue);
        Assert.Equal(expectedAuthenticationHeaderValueParameter, actualAuthenticationHeaderValue.ToString());
    }

    private static void AddInMemoryConfiguration(IServiceCollection services, Dictionary<string, string?> configurations)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configurations)
            .Build();

        services.AddScoped<IConfiguration>(_ => configuration);
    }
}

using System.Net.Http.Headers;
using Energinet.DataHub.Core.App.Common.Identity;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.Client.Extensions.Options;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
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
    public void AddMeasurementsClient_WhenOptionsAreConfiguredAndB2CAuthorizationHeaderProviderIsRegistered_ThenClientsCanBeCreated()
    {
        // Arrange
        var services = new ServiceCollection();
        var configurations = new Dictionary<string, string?>
        {
            [$"{MeasurementHttpClientOptions.SectionName}:{nameof(MeasurementHttpClientOptions.BaseAddress)}"] = "https://localhost",
            [$"{MeasurementHttpClientOptions.SectionName}:{nameof(MeasurementHttpClientOptions.ApplicationIdUri)}"] = "https://management.azure.com",
        };
        AddInMemoryConfiguration(services, configurations);

        // Act
        services.AddMeasurementsClient(new B2CAuthorizationHeaderProvider());

        // Assert
        var actual = services
            .BuildServiceProvider()
            .GetRequiredService<IMeasurementsClient>();
        Assert.IsType<MeasurementsClient>(actual);
    }

    private static void AddInMemoryConfiguration(IServiceCollection services, Dictionary<string, string?> configurations)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configurations)
            .Build();

        services.AddScoped<IConfiguration>(_ => configuration);
    }

    private class B2CAuthorizationHeaderProvider : IAuthorizationHeaderProvider
    {
        public AuthenticationHeaderValue CreateAuthorizationHeader(string scope)
        {
            return new AuthenticationHeaderValue("Bearer", "test-token");
        }
    }
}

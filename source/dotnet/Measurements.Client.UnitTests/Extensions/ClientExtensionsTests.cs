using Energinet.DataHub.Core.App.Common.Extensions.DependencyInjection;
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
        services.AddTokenCredentialProvider();
        services.AddMeasurementsClient();

        // Assert
        var actual = services
            .BuildServiceProvider()
            .GetRequiredService<IMeasurementsClient>();
        Assert.IsType<MeasurementsClient>(actual);
    }

    [Fact]
    public void AddMeasurementsClient_WhenTokenCredentialIsNotRegisteredAndOptionsAreConfigured_ThenExceptionIsThrownWhenRequestingClient()
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
        var serviceProvider = services.BuildServiceProvider();
        var exception = Assert.Throws<InvalidOperationException>(() => serviceProvider.GetRequiredService<IMeasurementsClient>());

        // Assert
        Assert.Contains(
            "No service for type 'Energinet.DataHub.Core.App.Common.Identity.TokenCredentialProvider' has been registered.",
            exception.Message);
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

    private static void AddInMemoryConfiguration(IServiceCollection services, Dictionary<string, string?> configurations)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configurations)
            .Build();

        services.AddScoped<IConfiguration>(_ => configuration);
    }
}

using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.Client.Extensions.Options;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.Client.Tests.Extensions;

[UnitTest]
public class ClientExtensionsTests
{
    [Fact]
    public void AddMeasurementsClient_WhenOptionsAreConfigured_ThenClientsCanBeCreated()
    {
        // Arrange
        var services = new ServiceCollection();
        AddInMemoryConfiguration(services);

        // Act
        services.AddMeasurementsClient();

        // Assert
        var actual = services
            .BuildServiceProvider()
            .GetRequiredService<IMeasurementsClient>();
        Assert.IsType<MeasurementsClient>(actual);
    }

    private static void AddInMemoryConfiguration(IServiceCollection services)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [$"{MeasurementHttpClientOptions.SectionName}:{nameof(MeasurementHttpClientOptions.BaseAddress)}"] = "https://localhost:7202",
            })
            .Build();

        services.AddScoped<IConfiguration>(_ => configuration);
    }
}

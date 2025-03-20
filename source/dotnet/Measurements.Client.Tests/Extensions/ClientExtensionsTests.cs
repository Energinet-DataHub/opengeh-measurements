using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.Client.Extensions.Options;
using Energinet.DataHub.Measurements.Client.Tests.Fixtures;
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
        services.AddInMemoryConfiguration(new Dictionary<string, string?>()
        {
            [$"{MeasurementHttpClientOptions.SectionName}:{nameof(MeasurementHttpClientOptions.BaseAddress)}"] = "https://localhost:7202",
        });

        // Act
        services.AddMeasurementsClient();

        // Assert
        var actual = services
            .BuildServiceProvider()
            .GetRequiredService<IMeasurementsClient>();
        Assert.IsType<MeasurementsClient>(actual);
    }
}

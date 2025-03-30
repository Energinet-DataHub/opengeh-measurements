using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.Client.Extensions.Options;
using Energinet.DataHub.Measurements.Client.IntegrationTests.Fixture;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.Client.IntegrationTests;

[IntegrationTest]
[Collection(nameof(MeasurementsClientCollection))]
public class MeasurementClientTests
{
    public MeasurementClientTests()
    {
        var services = new ServiceCollection();
        AddInMemoryConfiguration(services);
        services.AddMeasurementsClient();

        ServiceProvider = services.BuildServiceProvider();
    }

    private ServiceProvider ServiceProvider { get; }

    [Fact]
    public async Task GetMeasurementsForDayAsync_WhenCalled_ReturnsValidMeasurement()
    {
        // Arrange
        var measurementClient = ServiceProvider.GetRequiredService<IMeasurementsClient>();
        var query = new GetMeasurementsForDayQuery("1234567890", new LocalDate(2023, 1, 2));

        // Act
        var measurements = await measurementClient.GetMeasurementsForDayAsync(query);

        // Assert
        Assert.Equal(24, measurements.Count());
    }

    private void AddInMemoryConfiguration(IServiceCollection services)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [$"{MeasurementHttpClientOptions.SectionName}:{nameof(MeasurementHttpClientOptions.BaseAddress)}"] = "https://localhost:7202",
                [$"{AuthenticationOptions.SectionName}:{nameof(AuthenticationOptions.ApplicationIdUri)}"] = MeasurementsClientFixture.ApplicationIdUri,
                [$"{AuthenticationOptions.SectionName}:{nameof(AuthenticationOptions.Issuer)}"] = MeasurementsClientFixture.Issuer,
            })
            .Build();

        services.AddScoped<IConfiguration>(_ => configuration);
    }
}

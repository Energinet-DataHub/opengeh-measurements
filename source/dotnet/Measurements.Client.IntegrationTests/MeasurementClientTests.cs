using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.IntegrationTests.Fixture;
using Microsoft.Extensions.DependencyInjection;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.Client.IntegrationTests;

[IntegrationTest]
[Collection(nameof(MeasurementsClientCollection))]
public class MeasurementClientTests
{
    public MeasurementClientTests(MeasurementsClientFixture fixture)
    {
        Fixture = fixture;
    }

    private MeasurementsClientFixture Fixture { get; }

    [Fact]
    public async Task GetMeasurementsForDayAsync_WhenCalled_ReturnsValidMeasurement()
    {
        // Arrange
        // TODO: Move to fixture
        var query = new GetMeasurementsForDayQuery("1234567890", new LocalDate(2023, 1, 2));

        var measurementsClient = Fixture.ServiceProvider.GetRequiredService<IMeasurementsClient>();
        var measurements = await measurementsClient.GetMeasurementsForDayAsync(query);

        // Assert
        Assert.Equal(24, measurements.Count());
    }
}

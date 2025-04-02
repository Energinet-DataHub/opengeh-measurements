using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.IntegrationTests.Fixture;
using Microsoft.Extensions.DependencyInjection;
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
        var query = new GetMeasurementsForDayQuery(MeasurementsClientFixture.TestMeteringPointId, MeasurementsClientFixture.TestDate);

        var measurementsClient = Fixture.ServiceProvider.GetRequiredService<IMeasurementsClient>();
        var measurements = await measurementsClient.GetMeasurementsForDayAsync(query);

        // Assert
        Assert.Equal(24, measurements.Count());
    }
}

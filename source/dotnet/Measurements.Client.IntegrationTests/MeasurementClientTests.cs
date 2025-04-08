using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.IntegrationTests.Fixture;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.Client.IntegrationTests;

[IntegrationTest]
[Collection(nameof(MeasurementsClientCollection))]
public class MeasurementClientTests(MeasurementsClientFixture fixture)
{
    [Fact]
    public async Task GetMeasurementsForDayAsync_WhenCalled_ReturnsValidMeasurement()
    {
        // Arrange
        var query = new GetMeasurementsForDayQuery(MeasurementsClientFixture.TestMeteringPointId, MeasurementsClientFixture.TestDate);

        var measurementsClient = fixture.ServiceProvider.GetRequiredService<IMeasurementsClient>();
        var measurements = await measurementsClient.GetMeasurementsForDayAsync(query);

        // Assert
        Assert.Equal(24, measurements.MeasurementPositions.Count());
        AssertAllPointsInPositionsEqualsExpected(measurements, Quality.Measured);
    }

    private static void AssertAllPointsInPositionsEqualsExpected(MeasurementDto measurements, Quality expectedQuality)
    {
        foreach (var position in measurements.MeasurementPositions)
        {
            foreach (var point in position.MeasurementPoints)
            {
                Assert.Equal(expectedQuality, point.Quality);
            }
        }
    }
}

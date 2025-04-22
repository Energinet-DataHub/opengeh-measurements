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
    public async Task GetByDayAsync_WhenCalled_ReturnsValidMeasurement()
    {
        // Arrange
        var query = new GetByDayQuery(MeasurementsClientFixture.TestMeteringPointId, MeasurementsClientFixture.TestDate);

        var measurementsClient = fixture.ServiceProvider.GetRequiredService<IMeasurementsClient>();
        var measurements = await measurementsClient.GetByDayAsync(query);

        // Assert
        Assert.Equal(24, measurements.Count());
    }
}

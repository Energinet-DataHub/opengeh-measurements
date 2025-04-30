using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Client.IntegrationTests.Fixture;
using Microsoft.Extensions.DependencyInjection;
using NodaTime;
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
        var query = new GetByDayQuery(MeasurementsClientFixture.TestMeteringPointId, MeasurementsClientFixture.TestObservationDate);

        var measurementsClient = fixture.ServiceProvider.GetRequiredService<IMeasurementsClient>();
        var measurements = await measurementsClient.GetByDayAsync(query);

        // Assert
        Assert.Equal(24, measurements.MeasurementPositions.Count());
        AssertAllPointsInPositionsEqualsExpected(measurements);
    }

    [Fact]
    public async Task GetAggregatedByMonth_WhenCalled_ThenReturnsValidAggregatedMeasurements()
    {
        // Arrange
        var query = new GetAggregatedByDateQuery(
            MeasurementsClientFixture.TestMeteringPointId,
            new YearMonth(MeasurementsClientFixture.TestObservationDate.Year, MeasurementsClientFixture.TestObservationDate.Month));

        var measurementsClient = fixture.ServiceProvider.GetRequiredService<IMeasurementsClient>();
        var measurements = await measurementsClient.GetAggregatedByDate(query);

        // Assert
        Assert.Single(measurements);
    }

    [Fact]
    public async Task GetAggregatedByYear_WhenCalled_ThenReturnsValidAggregatedMeasurements()
    {
        // Arrange
        var query = new GetAggregatedByMonthQuery(
            MeasurementsClientFixture.TestMeteringPointId, MeasurementsClientFixture.TestObservationDate.Year);

        var measurementsClient = fixture.ServiceProvider.GetRequiredService<IMeasurementsClient>();
        var measurements = await measurementsClient.GetAggregatedByMonth(query);

        // Assert
        Assert.Single(measurements);
    }

    private static void AssertAllPointsInPositionsEqualsExpected(MeasurementDto measurements)
    {
        for (var positionIndex = 1; positionIndex < measurements.MeasurementPositions.Count(); positionIndex++)
        {
            var position = measurements.MeasurementPositions.ElementAt(positionIndex - 1);
            Assert.Equal(positionIndex, position.Index);

            for (var pointIndex = 1; pointIndex < position.MeasurementPoints.Count(); pointIndex++)
            {
                var point = position.MeasurementPoints.ElementAt(pointIndex);

                Assert.Equal(Quality.Measured, point.Quality);
                Assert.Equal(Resolution.Hourly, point.Resolution);
                Assert.Equal(Unit.kWh, point.Unit);
                Assert.Equal(pointIndex, point.Order);
                Assert.Equal("2025-01-17T03:40:55Z", point.PersistedTime.ToFormattedString());
                Assert.Equal("2025-01-17T03:40:55Z", point.RegistrationTime.ToFormattedString());
            }
        }
    }
}

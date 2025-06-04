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
    public async Task GetCurrentByPeriod_WhenCalled_ReturnsEmptyList()
    {
        // Arrange
        var query = new GetByPeriodQuery(
            MeasurementsClientFixture.TestMeteringPointId,
            Instant.FromDateTimeOffset(DateTimeOffset.UtcNow),
            Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddDays(1)));

        var measurementsClient = fixture.ServiceProvider.GetRequiredService<IMeasurementsClient>();
        var measurements = await measurementsClient.GetCurrentByPeriodAsync(query);

        // Assert
        Assert.Empty(measurements);
    }

    [Fact]
    public async Task GetMonthlyAggregateByDateAsync_WhenCalled_ThenReturnsValidAggregatedMeasurements()
    {
        // Arrange
        var query = new GetMonthlyAggregateByDateQuery(
            MeasurementsClientFixture.TestMeteringPointId,
            new YearMonth(MeasurementsClientFixture.TestObservationDate.Year, MeasurementsClientFixture.TestObservationDate.Month));
        var measurementsClient = fixture.ServiceProvider.GetRequiredService<IMeasurementsClient>();

        // Act
        var measurements = await measurementsClient.GetMonthlyAggregateByDateAsync(query);

        // Assert
        var actual = measurements.Single();
        Assert.Equal(MeasurementsClientFixture.TestObservationDate.Year, actual.Date.Year);
        Assert.Equal(MeasurementsClientFixture.TestObservationDate.Month, actual.Date.Month);
        Assert.False(actual.IsMissingValues);
        Assert.False(actual.ContainsUpdatedValues);
        Assert.Contains(Quality.Measured, actual.Qualities);
        Assert.Equal(285.6m, actual.Quantity);
    }

    [Fact]
    public async Task GetYearlyAggregateByMonthAsync_WhenCalled_ThenReturnsValidAggregatedMeasurements()
    {
        // Arrange
        var query = new GetYearlyAggregateByMonthQuery(
            MeasurementsClientFixture.TestMeteringPointId, MeasurementsClientFixture.TestObservationDate.Year);

        var measurementsClient = fixture.ServiceProvider.GetRequiredService<IMeasurementsClient>();
        var measurements = await measurementsClient.GetYearlyAggregateByMonthAsync(query);

        // Assert
        Assert.Single(measurements);
    }

    [Fact]
    public async Task GetAggregateByYearAsync_WhenCalled_ThenReturnsValidAggregatedMeasurements()
    {
        // Arrange
        var query = new GetAggregateByYearQuery(MeasurementsClientFixture.TestMeteringPointId);

        var measurementsClient = fixture.ServiceProvider.GetRequiredService<IMeasurementsClient>();
        var measurements = await measurementsClient.GetAggregateByYearAsync(query);

        // Assert
        Assert.Single(measurements);
    }

    [Fact]
    public async Task GetAggregatedByPeriod_WhenCalled_ThenReturnsValidAggregatedMeasurements()
    {
        // Arrange
        var query = new GetAggregateByPeriodQuery(
            [MeasurementsClientFixture.TestMeteringPointId],
            Instant.FromDateTimeOffset(MeasurementsClientFixture.TestObservationDate.ToUtcDateTimeOffset()),
            Instant.FromDateTimeOffset(MeasurementsClientFixture.TestObservationDate.ToUtcDateTimeOffset().AddDays(1)),
            Aggregation.Hour);

        var measurementsClient = fixture.ServiceProvider.GetRequiredService<IMeasurementsClient>();
        var measurements = await measurementsClient.GetAggregatedByPeriodAsync(query);

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

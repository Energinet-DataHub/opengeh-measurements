using AutoFixture.Xunit2;
using Energinet.DataHub.Core.TestCommon.AutoFixture.Attributes;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Client.Factories;
using Energinet.DataHub.Measurements.Client.Models;
using NodaTime;

namespace Energinet.DataHub.Measurements.Client.UnitTests.Factories;

public class MeasurementAggregationFactoryTests
{
    [Theory]
    [InlineAutoMoqData(new[] { Quality.Measured }, Quality.Measured)]
    [InlineAutoMoqData(new[] { Quality.Measured, Quality.Calculated }, Quality.Calculated)]
    [InlineAutoMoqData(new[] { Quality.Measured, Quality.Calculated, Quality.Estimated }, Quality.Estimated)]
    [InlineAutoMoqData(new[] { Quality.Measured, Quality.Calculated, Quality.Estimated, Quality.Missing }, Quality.Missing)]
    public void Create_ValidInput_ReturnsExpectedQuality(
        Quality[] qualities,
        Quality expectedQuality,
        MeasurementAggregationFactory sut)
    {
        // Arrange
        var minObservationTime = DateTimeOffset.UtcNow;
        var maxObservationTime = DateTimeOffset.UtcNow.AddHours(23);
        var date = LocalDate.FromDateTime(minObservationTime.DateTime);
        const decimal quantity = 100.0m;
        var resolutions = new List<Resolution> { Resolution.PT1H };
        const int pointCount = 24;
        var aggregatedMeasurements = new AggregatedMeasurements(
            minObservationTime,
            maxObservationTime,
            quantity,
            qualities,
            resolutions,
            pointCount);

        // Act
        var result = sut.Create(aggregatedMeasurements, date);

        // Assert
        Assert.Equal(date, result.Date);
        Assert.Equal(expectedQuality, result.Quality);
        Assert.Equal(100.0m, result.Quantity);
        Assert.False(result.MissingValues);
    }

    [Theory]
    [InlineAutoMoqData(23, true)]
    [InlineAutoMoqData(24, false)]
    public void Create_ValidInput_ReturnsExpectedMissingValues(
        int pointCount,
        bool missingValues,
        MeasurementAggregationFactory sut)
    {
        // Arrange
        var minObservationTime = DateTimeOffset.UtcNow;
        var maxObservationTime = DateTimeOffset.UtcNow.AddHours(23);
        var date = LocalDate.FromDateTime(minObservationTime.DateTime);
        const decimal quantity = 100.0m;
        var qualities = new List<Quality> { Quality.Measured };
        var resolutions = new List<Resolution> { Resolution.PT1H };

        var aggregatedMeasurements = new AggregatedMeasurements(
            minObservationTime,
            maxObservationTime,
            quantity,
            qualities,
            resolutions,
            pointCount);

        // Act
        var result = sut.Create(aggregatedMeasurements, date);

        // Assert
        Assert.Equal(date, result.Date);
        Assert.Equal(Quality.Measured, result.Quality);
        Assert.Equal(100.0m, result.Quantity);
        Assert.Equal(missingValues, result.MissingValues);
    }

    [Theory]
    [AutoMoqData]
    public void Create_EmptyAggregatedMeasurement_ReturnsExpectedMissingValues(MeasurementAggregationFactory sut)
    {
        // Arrange
        var minObservationTime = DateTimeOffset.UtcNow;
        var date = LocalDate.FromDateTime(minObservationTime.DateTime);

        // Act
        var result = sut.Create(null, date);

        // Assert
        Assert.Equal(date, result.Date);
        Assert.Equal(Quality.Missing, result.Quality);
        Assert.Equal(0.0m, result.Quantity);
        Assert.True(result.MissingValues);
    }
}

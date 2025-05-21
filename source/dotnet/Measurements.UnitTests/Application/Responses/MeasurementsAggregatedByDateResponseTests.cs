using System.Dynamic;
using Energinet.DataHub.Measurements.Application.Extensions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Application.Responses;

[UnitTest]
public class MeasurementsAggregatedByDateResponseTests
{
    [Fact]
    public void Create_WhenValidInput_ReturnExpectedResult()
    {
        // Arrange
        var minObservationTime = Instant.FromUtc(2022, 2, 5, 23, 0, 0);
        var maxObservationTime = Instant.FromUtc(2022, 2, 6, 22, 0, 0);

        var qualities = new[] { "measured" };
        var resolutions = new[] { "PT1H" };
        var units = new[] { "kWh" };

        var expectedDate = minObservationTime.ToDateOnly();

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, resolutions, units)),
        };

        // Act
        var actual = MeasurementsAggregatedByDateResponse.Create(aggregatedMeasurements);

        // Assert
        var firstAggregation = actual.MeasurementAggregations.First();
        Assert.Equal(expectedDate, firstAggregation.Date);
        Assert.Equal(100.0m, firstAggregation.Quantity);
        Assert.Equal(Quality.Measured, firstAggregation.Quality);
        Assert.False(firstAggregation.MissingValues);
        Assert.False(firstAggregation.ContainsUpdatedValues);
    }

    [Fact]
    public void Create_WhenMultipleResolutions_ThenThrowException()
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        var qualities = new[] { "measured", "measured" };
        var resolutions = new[] { "PT1H", "PT15M" };
        var units = new[] { "kWh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, resolutions, units)),
        };

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => MeasurementsAggregatedByDateResponse.Create(aggregatedMeasurements));
    }

    [Fact]
    public void Create_WhenMultipleUnits_ThenThrowException()
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        var qualities = new[] { "measured", "measured" };
        var resolutions = new[] { "PT1H" };
        var units = new[] { "kWh", "kVArh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, resolutions, units)),
        };

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => MeasurementsAggregatedByDateResponse.Create(aggregatedMeasurements));
    }

    [Fact]
    public void Create_WhenMultipleQualities_ThenLowestQualityIsReturned()
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        var qualities = new[] { "measured", "estimated", "calculated", "missing" };
        var resolutions = new[] { "PT1H" };
        var units = new[] { "kWh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, resolutions, units)),
        };

        // Act
        var actual = MeasurementsAggregatedByDateResponse.Create(aggregatedMeasurements);

        // Assert
        var firstAggregation = actual.MeasurementAggregations.First();
        Assert.Equal(Quality.Missing, firstAggregation.Quality);
    }

    [Theory]
    [InlineData(15, Quality.Measured, true)]
    [InlineData(15, Quality.Missing, true)]
    [InlineData(24, Quality.Measured, false)]
    [InlineData(24, Quality.Missing, true)]
    public void Create_WhenMeasurementIsMissingValues_ThenMissingValuesFlagIsSet(int observationPointsCount, Quality quality, bool expectedMissingValues)
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(observationPointsCount));
        var qualities = new[] { quality.ToString() };
        var resolutions = new[] { "PT1H" };
        var units = new[] { "kWh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, resolutions, units, observationPointsCount)),
        };

        // Act
        var actual = MeasurementsAggregatedByDateResponse.Create(aggregatedMeasurements);

        // Assert
        Assert.Equal(expectedMissingValues, actual.MeasurementAggregations.Single().MissingValues);
    }

    [Fact]
    public void Create_WhenMeasurementContainsMissingObservations_ThenMissingValuesIsTrue()
    {
        // Arrange
        const int observationPointsCount = 15;
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(observationPointsCount));
        var qualities = new[] { "measured" };
        var resolutions = new[] { "PT1H" };
        var units = new[] { "kWh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, resolutions, units, pointCount: observationPointsCount)),
        };

        // Act
        var actual = MeasurementsAggregatedByDateResponse.Create(aggregatedMeasurements);

        // Assert
        Assert.True(actual.MeasurementAggregations.Single().MissingValues);
    }

    [Fact]
    public void Create_WhenQualityIsQuantityMissing_ThenMissingValuesIsTrue()
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(24));
        var qualities = new[] { "missing" };
        var resolutions = new[] { "PT1H" };
        var units = new[] { "kWh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, resolutions, units)),
        };

        // Act
        var actual = MeasurementsAggregatedByDateResponse.Create(aggregatedMeasurements);

        // Assert
        Assert.True(actual.MeasurementAggregations.Single().MissingValues);
    }

    [Fact]
    public void Create_WhenDataContainsInvalidQualities_ThenThrowException()
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        var qualities = new[] { "invalid_quality" };
        var resolutions = new[] { "PT1H" };
        var units = new[] { "kWh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, resolutions, units)),
        };

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => MeasurementsAggregatedByDateResponse.Create(aggregatedMeasurements));
    }

    [Fact]
    public void Create_WhenDataContainsUpdatedObservations_ThenContainsUpdatedValuesIsTrue()
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        var qualities = new[] { "measured" };
        var resolutions = new[] { "PT1H" };
        var units = new[] { "kWh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, resolutions, units, observationUpdates: 2)),
        };

        // Act
        var actual = MeasurementsAggregatedByDateResponse.Create(aggregatedMeasurements);

        // Assert
        Assert.True(actual.MeasurementAggregations.First().ContainsUpdatedValues);
    }

    private static ExpandoObject CreateRaw(
        Instant minObservationTime,
        Instant maxObservationTime,
        string[] qualities,
        string[] resolutions,
        string[] units,
        int pointCount = 24,
        int observationUpdates = 1)
    {
        dynamic raw = new ExpandoObject();
        raw.min_observation_time = minObservationTime.ToDateTimeOffset();
        raw.max_observation_time = maxObservationTime.ToDateTimeOffset();
        raw.aggregated_quantity = 100.0m;
        raw.qualities = qualities;
        raw.resolutions = resolutions;
        raw.units = units;
        raw.point_count = pointCount;
        raw.observation_updates = observationUpdates;

        return raw;
    }
}

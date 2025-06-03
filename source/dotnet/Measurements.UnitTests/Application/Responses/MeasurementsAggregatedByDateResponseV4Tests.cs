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
public class MeasurementsAggregatedByDateResponseV4Tests
{
    [Fact]
    [Obsolete("Obsolete. Delete when API version 4.0 is removed.")]
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
        var actual = MeasurementsAggregatedByDateResponseV4.Create(aggregatedMeasurements)!;

        // Assert
        var firstAggregation = actual.MeasurementAggregations.First();
        Assert.Equal(expectedDate, firstAggregation.Date);
        Assert.Equal(100.0m, firstAggregation.Quantity);
        Assert.Equal(Quality.Measured, firstAggregation.Quality);
        Assert.False(firstAggregation.IsMissingValues);
        Assert.False(firstAggregation.ContainsUpdatedValues);
    }

    [Fact]
    [Obsolete("Obsolete. Delete when API version 4.0 is removed.")]
    public void Create_WhenDuplicateResolutions_ThenThrowException()
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
        Assert.Throws<InvalidOperationException>(() => MeasurementsAggregatedByDateResponseV4.Create(aggregatedMeasurements)!);
    }

    [Fact]
    [Obsolete("Obsolete. Delete when API version 4.0 is removed.")]
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
        Assert.Throws<InvalidOperationException>(() => MeasurementsAggregatedByDateResponseV4.Create(aggregatedMeasurements)!);
    }

    [Fact]
    [Obsolete("Obsolete. Delete when API version 4.0 is removed.")]
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
        var actual = MeasurementsAggregatedByDateResponseV4.Create(aggregatedMeasurements)!;

        // Assert
        var firstAggregation = actual.MeasurementAggregations.First();
        Assert.Equal(Quality.Missing, firstAggregation.Quality);
    }

    [Theory]
    [InlineData(null!)]
    [InlineData(0)]
    [Obsolete("Obsolete. Delete when API version 4.0 is removed.")]
    public void Create_WhenDataContainsQualityMissing_ThenQuantityCanBeZeroOrNull(int? quantity)
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        var qualities = new[] { "measured", "missing" };
        var resolutions = new[] { "PT1H" };
        var units = new[] { "kWh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, resolutions, units, quantity: quantity)),
        };

        // Act
        var actual = MeasurementsAggregatedByDateResponseV4.Create(aggregatedMeasurements)!;

        // Assert
        var firstAggregation = actual.MeasurementAggregations.First();
        Assert.Equal(quantity, firstAggregation.Quantity);
    }

    [Theory]
    [InlineData(15, Quality.Measured, true)]
    [InlineData(15, Quality.Calculated, true)]
    [InlineData(15, Quality.Estimated, true)]
    [InlineData(15, Quality.Missing, true)]
    [InlineData(24, Quality.Measured, false)]
    [InlineData(24, Quality.Calculated, false)]
    [InlineData(24, Quality.Estimated, false)]
    [InlineData(24, Quality.Missing, true)]
    [Obsolete("Obsolete. Delete when API version 4.0 is removed.")]
    public void Create_WhenObservationsOrQualityIsMissing_ThenIsMissingValuesFlagIsSet(long observationPointsCount, Quality quality, bool expectedMissingValues)
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(observationPointsCount));
        var qualities = new[] { quality.ToString() };
        var resolutions = new[] { "PT1H" };
        var units = new[] { "kWh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, resolutions, units, pointCount: observationPointsCount)),
        };

        // Act
        var actual = MeasurementsAggregatedByDateResponseV4.Create(aggregatedMeasurements)!;

        // Assert
        Assert.Equal(expectedMissingValues, actual.MeasurementAggregations.Single().IsMissingValues);
    }

    [Fact]
    [Obsolete("Obsolete. Delete when API version 4.0 is removed.")]
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
        Assert.Throws<ArgumentOutOfRangeException>(() => MeasurementsAggregatedByDateResponseV4.Create(aggregatedMeasurements)!);
    }

    [Fact]
    [Obsolete("Obsolete. Delete when API version 4.0 is removed.")]
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
        var actual = MeasurementsAggregatedByDateResponseV4.Create(aggregatedMeasurements)!;

        // Assert
        Assert.True(actual.MeasurementAggregations.First().ContainsUpdatedValues);
    }

    private static ExpandoObject CreateRaw(
        Instant minObservationTime,
        Instant maxObservationTime,
        string[] qualities,
        string[] resolutions,
        string[] units,
        long pointCount = 24L,
        long observationUpdates = 1L,
        decimal? quantity = 100.0m)
    {
        dynamic raw = new ExpandoObject();
        raw.min_observation_time = minObservationTime.ToDateTimeOffset();
        raw.max_observation_time = maxObservationTime.ToDateTimeOffset();
        raw.aggregated_quantity = quantity;
        raw.qualities = qualities;
        raw.resolutions = resolutions;
        raw.units = units;
        raw.point_count = pointCount;
        raw.observation_updates = observationUpdates;

        return raw;
    }
}

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
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));

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

    [Fact]
    public void Create_WhenDataContainsMissingValues_ThenMissingValuesIsTrue()
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(24)); // 24 hours to simulate missing values
        var qualities = new[] { "measured" };
        var resolutions = new[] { "PT1H" };
        var units = new[] { "kWh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, resolutions, units)),
        };

        // Act
        var actual = MeasurementsAggregatedByDateResponse.Create(aggregatedMeasurements);

        // Assert
        Assert.True(actual.MeasurementAggregations.First().MissingValues);
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
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, resolutions, units, 2L)),
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
        long observationUpdates = 1L)
    {
        dynamic raw = new ExpandoObject();
        raw.min_observation_time = minObservationTime.ToDateTimeOffset();
        raw.max_observation_time = maxObservationTime.ToDateTimeOffset();
        raw.aggregated_quantity = 100.0m;
        raw.qualities = qualities;
        raw.resolutions = resolutions;
        raw.units = units;
        raw.point_count = 24L;
        raw.observation_updates = observationUpdates;

        return raw;
    }
}

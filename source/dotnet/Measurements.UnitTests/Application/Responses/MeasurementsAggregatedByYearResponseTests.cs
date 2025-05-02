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
public class MeasurementsAggregatedByYearResponseTests
{
    [Fact]
    public void Create_WhenValidInput_ThenExpectedResultIsReturned()
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));

        var qualities = new[] { "measured" };
        var units = new[] { "kWh" };

        var expectedYear = minObservationTime.ToDateOnly().Year;

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, units)),
        };

        // Act
        var actual = MeasurementsAggregatedByYearResponse.Create(aggregatedMeasurements);

        // Assert
        var firstAggregation = actual.MeasurementAggregations.First();
        Assert.Equal(expectedYear, firstAggregation.Year);
        Assert.Equal(100.0m, firstAggregation.Quantity);
        Assert.Equal(Quality.Measured, firstAggregation.Quality);
    }

    [Fact]
    public void Create_WhenMultipleUnits_ThenFirstUnitIsReturned()
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        var qualities = new[] { "measured" };
        var units = new[] { "kWh", "kVArh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, units)),
        };

        // Act
        var actual = MeasurementsAggregatedByYearResponse.Create(aggregatedMeasurements);

        // Assert
        var firstAggregation = actual.MeasurementAggregations.First();
        Assert.Equal(Unit.kWh, firstAggregation.Unit);
    }

    [Fact]
    public void Create_WhenMultipleQualities_ThenLowestQualityIsReturned()
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        var qualities = new[] { "measured", "estimated", "calculated", "missing" };
        var units = new[] { "kWh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, units)),
        };

        // Act
        var actual = MeasurementsAggregatedByYearResponse.Create(aggregatedMeasurements);

        // Assert
        var firstAggregation = actual.MeasurementAggregations.First();
        Assert.Equal(Quality.Missing, firstAggregation.Quality);
    }

    [Fact]
    public void Create_WhenDataContainsInvalidQualities_ThenThrowException()
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        var qualities = new[] { "invalid_quality" };
        var units = new[] { "kWh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, units)),
        };

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => MeasurementsAggregatedByYearResponse.Create(aggregatedMeasurements));
    }

    private static ExpandoObject CreateRaw(
        Instant minObservationTime,
        Instant maxObservationTime,
        string[] qualities,
        string[] units,
        long observationUpdates = 1L)
    {
        dynamic raw = new ExpandoObject();
        raw.min_observation_time = minObservationTime.ToDateTimeOffset();
        raw.max_observation_time = maxObservationTime.ToDateTimeOffset();
        raw.aggregated_quantity = 100.0m;
        raw.qualities = qualities;
        raw.resolutions = new[] { "PT1H" };
        raw.units = units;
        raw.point_count = 24L;
        raw.observation_updates = observationUpdates;

        return raw;
    }
}

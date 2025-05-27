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
public class MeasurementsAggregatedByMonthResponseTests
{
    [Fact]
    public void Create_WhenValidInput_ReturnExpectedResult()
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));

        var qualities = new[] { "measured" };
        var units = new[] { "kWh" };

        var expectedYearMonth = new YearMonth(minObservationTime.ToDateOnly().Year, minObservationTime.ToDateOnly().Month);

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, units)),
        };

        // Act
        var actual = MeasurementsAggregatedByMonthResponse.Create(aggregatedMeasurements);

        // Assert
        var firstAggregation = actual.MeasurementAggregations.First();
        Assert.Equal(expectedYearMonth, firstAggregation.YearMonth);
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
        var actual = MeasurementsAggregatedByMonthResponse.Create(aggregatedMeasurements);

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
        var actual = MeasurementsAggregatedByMonthResponse.Create(aggregatedMeasurements);

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
        Assert.Throws<ArgumentOutOfRangeException>(() => MeasurementsAggregatedByMonthResponse.Create(aggregatedMeasurements));
    }

    [Theory]
    [InlineData("2023-01-31T23:00:00Z", 300L, Quality.Missing, true)]
    [InlineData("2023-01-31T23:00:00Z", 300L, Quality.Estimated, true)]
    [InlineData("2023-01-31T23:00:00Z", 300L, Quality.Calculated, true)]
    [InlineData("2023-01-31T23:00:00Z", 300L, Quality.Measured, true)]
    [InlineData("2023-01-31T23:00:00Z", 672L, Quality.Missing, true)]
    [InlineData("2023-01-31T23:00:00Z", 672L, Quality.Estimated, false)]
    [InlineData("2023-01-31T23:00:00Z", 672L, Quality.Calculated, false)]
    [InlineData("2023-01-31T23:00:00Z", 672L, Quality.Measured, false)]
    public void Create_WhenObservationsOrQualityIsMissing_ThenMissingValuesFlagIsSet(
        string timestamp, long observationPointsCount, Quality quality, bool expectedMissingValues)
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.Parse(timestamp));
        var maxObservationTime = minObservationTime.Plus(Duration.FromDays(observationPointsCount));
        var qualities = new[] { quality.ToString() };
        var units = new[] { "kWh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, units, observationPointsCount: observationPointsCount)),
        };

        // Act
        var actual = MeasurementsAggregatedByMonthResponse.Create(aggregatedMeasurements);

        // Assert
        Assert.Equal(expectedMissingValues, actual.MeasurementAggregations.Single().IsMissingValues);
    }

    private static ExpandoObject CreateRaw(
        Instant minObservationTime,
        Instant maxObservationTime,
        string[] qualities,
        string[] units,
        long observationPointsCount = 744L,
        long observationUpdates = 1L)
    {
        dynamic raw = new ExpandoObject();
        raw.min_observation_time = minObservationTime.ToDateTimeOffset();
        raw.max_observation_time = maxObservationTime.ToDateTimeOffset();
        raw.aggregated_quantity = 100.0m;
        raw.qualities = qualities;
        raw.resolutions = new[] { "PT1H" };
        raw.units = units;
        raw.point_count = observationPointsCount;
        raw.observation_updates = observationUpdates;

        return raw;
    }
}

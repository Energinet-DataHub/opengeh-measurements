using System.Dynamic;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Application.Responses;

[UnitTest]
public class MeasurementsAggregatedByPeriodResponseTests
{
    [Fact]
    public void Create_WhenValidInput_ReturnExpectedResult()
    {
        // Arrange
        const string meteringPoints = "123456789012345678";
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        const decimal aggregatedQuantity = 100.0m;
        var qualities = new[] { "measured" };
        const string resolution = "PT1H";
        const string aggregationGroupKey = "12345678901234567890";

        var aggregatedMeasurements = new List<AggregatedByPeriodMeasurementsResult>
        {
            new(CreateRaw(
                meteringPoints,
                minObservationTime,
                maxObservationTime,
                aggregatedQuantity,
                qualities,
                resolution,
                aggregationGroupKey)),
        };

        // Act
        var actual = MeasurementsAggregatedByPeriodResponse.Create(aggregatedMeasurements);

        // Assert
        var firstAggregation = actual.MeasurementAggregations.First();
        Assert.Single(firstAggregation.PointAggregationGroups);
        Assert.Equal(meteringPoints, firstAggregation.MeteringPoint.Id);
        Assert.Equal(minObservationTime, firstAggregation.PointAggregationGroups.First().Value.From);
        Assert.Equal(maxObservationTime, firstAggregation.PointAggregationGroups.First().Value.To);
        Assert.Equal(aggregatedQuantity, firstAggregation.PointAggregationGroups.First().Value.PointAggregations.First().Quantity);
        Assert.Equal(Quality.Measured, firstAggregation.PointAggregationGroups.First().Value.PointAggregations.First().Quality);
        Assert.Equal(Resolution.Hourly, firstAggregation.PointAggregationGroups.First().Value.Resolution);
    }

    [Fact]
    public void Create_WhenMultipleQualities_ThenLowestQualityIsReturned()
    {
        // Arrange
        const string meteringPoints = "123456789012345678";
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        const decimal aggregatedQuantity = 100.0m;
        var qualities = new[] { "measured", "estimated" };
        const string resolution = "PT1H";
        const string aggregationGroupKey = "12345678901234567890";

        var aggregatedMeasurements = new List<AggregatedByPeriodMeasurementsResult>
        {
            new(CreateRaw(
                meteringPoints,
                minObservationTime,
                maxObservationTime,
                aggregatedQuantity,
                qualities,
                resolution,
                aggregationGroupKey)),
        };

        // Act
        var actual = MeasurementsAggregatedByPeriodResponse.Create(aggregatedMeasurements);

        // Assert
        var firstAggregation = actual.MeasurementAggregations.First();
        Assert.Equal(Quality.Estimated, firstAggregation.PointAggregationGroups.First().Value.PointAggregations.First().Quality);
    }

    [Fact]
    public void Create_WhenDataContainsInvalidQualities_ThenThrowException()
    {
        // Arrange
        const string meteringPoints = "123456789012345678";
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        const decimal aggregatedQuantity = 100.0m;
        var qualities = new[] { "invalid_quality" };
        const string resolution = "PT1H";
        const string aggregationGroupKey = "12345678901234567890";

        var aggregatedMeasurements = new List<AggregatedByPeriodMeasurementsResult>
        {
            new(CreateRaw(
                meteringPoints,
                minObservationTime,
                maxObservationTime,
                aggregatedQuantity,
                qualities,
                resolution,
                aggregationGroupKey)),
        };

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => MeasurementsAggregatedByPeriodResponse.Create(aggregatedMeasurements));
    }

    [Fact]
    public void Create_WhenDataContainsMultipleRows_MultiplePointAggregationsIsReturned()
    {
        // Arrange
        const string meteringPoints = "123456789012345678";
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        const decimal aggregatedQuantity = 100.0m;
        var qualities = new[] { "measured" };
        const string resolution = "PT1H";
        const string aggregationGroupKey = "12345678901234567890";

        var aggregatedMeasurements = new List<AggregatedByPeriodMeasurementsResult>
        {
            new(CreateRaw(
                meteringPoints,
                minObservationTime,
                maxObservationTime,
                aggregatedQuantity,
                qualities,
                resolution,
                aggregationGroupKey)),
            new(CreateRaw(
                meteringPoints,
                minObservationTime.Plus(Duration.FromDays(1)),
                maxObservationTime.Plus(Duration.FromDays(1)),
                aggregatedQuantity * 2,
                qualities,
                resolution,
                aggregationGroupKey)),
        };

        // Act
        var actual = MeasurementsAggregatedByPeriodResponse.Create(aggregatedMeasurements);

        // Assert
        Assert.Equal(2, actual.MeasurementAggregations.First().PointAggregationGroups.First().Value.PointAggregations.Count);
    }

    [Fact]
    public void Create_WhenRequestContainsMultipleMeteringPoints_MultipleAggregationsIsReturned()
    {
        // Arrange
        const string meteringPoints1 = "123456789012345678";
        const string meteringPoints2 = "123456789012345679";
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        const decimal aggregatedQuantity = 100.0m;
        var qualities = new[] { "measured" };
        const string resolution = "PT1H";
        const string aggregationGroupKey = "12345678901234567890";
        var aggregatedMeasurements = new List<AggregatedByPeriodMeasurementsResult>
        {
            new(CreateRaw(
                meteringPoints1,
                minObservationTime,
                maxObservationTime,
                aggregatedQuantity,
                qualities,
                resolution,
                aggregationGroupKey)),
            new(CreateRaw(
                meteringPoints2,
                minObservationTime,
                maxObservationTime,
                aggregatedQuantity * 2,
                qualities,
                resolution,
                aggregationGroupKey)),
        };

        // Act
        var actual = MeasurementsAggregatedByPeriodResponse.Create(aggregatedMeasurements);

        // Assert
        Assert.Equal(2, actual.MeasurementAggregations.Count);
    }

    [Fact]
    public void Create_WhenDataContainsMultipleRowsWithDifferentResolution_MultiplePointAggregationGroupsIsReturned()
    {
        // Arrange
        const string meteringPoints = "123456789012345678";
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        const decimal aggregatedQuantity = 100.0m;
        var qualities = new[] { "measured" };
        const string resolution1 = "PT1H";
        const string resolution2 = "PT15M";
        const string aggregationGroupKey = "12345678901234567890";

        var aggregatedMeasurements = new List<AggregatedByPeriodMeasurementsResult>
        {
            new(CreateRaw(
                meteringPoints,
                minObservationTime,
                maxObservationTime,
                aggregatedQuantity,
                qualities,
                resolution1,
                aggregationGroupKey)),
            new(CreateRaw(
                meteringPoints,
                minObservationTime.Plus(Duration.FromDays(1)),
                maxObservationTime.Plus(Duration.FromDays(1)),
                aggregatedQuantity * 2,
                qualities,
                resolution2,
                aggregationGroupKey)),
        };

        // Act
        var actual = MeasurementsAggregatedByPeriodResponse.Create(aggregatedMeasurements);

        // Assert
        Assert.Equal(2, actual.MeasurementAggregations.First().PointAggregationGroups.Count);
    }

    private static ExpandoObject CreateRaw(
        string meteringPoints,
        Instant minObservationTime,
        Instant maxObservationTime,
        decimal aggregatedQuantity,
        string[] qualities,
        string resolution,
        string aggregationGroupKey)
    {
        dynamic raw = new ExpandoObject();
        raw.metering_point_id = meteringPoints;
        raw.min_observation_time = minObservationTime.ToDateTimeOffset();
        raw.max_observation_time = maxObservationTime.ToDateTimeOffset();
        raw.aggregated_quantity = aggregatedQuantity;
        raw.qualities = qualities;
        raw.resolution = resolution;
        raw.aggregation_group_key = aggregationGroupKey;

        return (ExpandoObject)raw;
    }
}

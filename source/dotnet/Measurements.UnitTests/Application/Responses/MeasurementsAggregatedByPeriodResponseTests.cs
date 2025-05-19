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
        Assert.Equal(aggregatedQuantity, firstAggregation.PointAggregationGroups.First().Value.PointAggregations.First().AggregatedQuantity);
        Assert.Equal(Quality.Measured, firstAggregation.PointAggregationGroups.First().Value.PointAggregations.First().Quality);
        Assert.Equal(Resolution.Hourly, firstAggregation.PointAggregationGroups.First().Value.Resolution);
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

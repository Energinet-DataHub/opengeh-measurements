using System.Dynamic;
using Energinet.DataHub.Measurements.Application.Extensions;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using NodaTime;
using Xunit;

namespace Energinet.DataHub.Measurements.UnitTests.Application.Responses;

public class GetAggregatedMeasurementResponseTests
{
    [Fact]
    public void Create_ValidInput_ReturnsExpectedResult()
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));

        var qualities = new[] { "measured" };
        var resolutions = new[] { "PT1H" };

        var expectedDate = minObservationTime.ToDateOnly();

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>()
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, resolutions)),
        };

        // Act
        var result = GetAggregatedMeasurementsResponse.Create(aggregatedMeasurements);

        // Assert
        var firstAggregation = result.MeasurementAggregations.First();
        Assert.Equal(expectedDate, firstAggregation.Date);
        Assert.Equal(100.0m, firstAggregation.Quantity);
        Assert.Equal(Quality.Measured, firstAggregation.Quality);
        Assert.False(firstAggregation.MissingValues);
    }

    [Fact]
    public void Create_MultipleResolutions_ShouldThrowException()
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        var qualities = new[] { "measured", "measured" };
        var resolutions = new[] { "PT1H", "PT15M" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>()
        {
            new(CreateRaw(minObservationTime, maxObservationTime, qualities, resolutions)),
        };

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() => GetAggregatedMeasurementsResponse.Create(aggregatedMeasurements));
    }

    /* TODO: Test for missing values, invalid qualities, and other edge cases */

    private static ExpandoObject CreateRaw(
        Instant minObservationTime, Instant maxObservationTime, string[] qualities, string[] resolutions)
    {
        dynamic raw = new ExpandoObject();
        raw.min_observation_time = minObservationTime.ToDateTimeOffset();
        raw.max_observation_time = maxObservationTime.ToDateTimeOffset();
        raw.aggregated_quantity = 100.0m;
        raw.qualities = qualities;
        raw.resolutions = resolutions;
        raw.point_count = 24L;

        return raw;
    }
}

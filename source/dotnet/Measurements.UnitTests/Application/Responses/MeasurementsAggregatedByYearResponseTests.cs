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
        var expectedYear = minObservationTime.ToDateOnly().Year;
        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime)),
        };

        // Act
        var actual = MeasurementsAggregatedByYearResponse.Create(aggregatedMeasurements);

        // Assert
        var firstAggregation = actual.MeasurementAggregations.First();
        Assert.Equal(expectedYear, firstAggregation.Year);
        Assert.Equal(100.0m, firstAggregation.Quantity);
    }

    [Fact]
    public void Create_WhenMultipleUnits_ThenThrowException()
    {
        // Arrange
        var minObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var maxObservationTime = Instant.FromDateTimeOffset(DateTimeOffset.UtcNow.AddHours(23));
        var units = new[] { "kWh", "kVArh" };

        var aggregatedMeasurements = new List<AggregatedMeasurementsResult>
        {
            new(CreateRaw(minObservationTime, maxObservationTime, units)),
        };

        // Act
        Assert.Throws<InvalidOperationException>(() => MeasurementsAggregatedByYearResponse.Create(aggregatedMeasurements));
    }

    private static ExpandoObject CreateRaw(
        Instant minObservationTime,
        Instant maxObservationTime,
        string[]? units = null)
    {
        dynamic raw = new ExpandoObject();
        raw.min_observation_time = minObservationTime.ToDateTimeOffset();
        raw.max_observation_time = maxObservationTime.ToDateTimeOffset();
        raw.aggregated_quantity = 100.0m;
        raw.units = units ?? ["kWh"];

        return raw;
    }
}

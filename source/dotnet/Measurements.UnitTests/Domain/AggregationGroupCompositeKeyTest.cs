using Energinet.DataHub.Measurements.Domain;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Domain;

[UnitTest]
public class AggregationGroupCompositeKeyTest
{
    [Fact]
    public void AggregationGroupCompositeKey_BuildsKeyCorrectly()
    {
        // Arrange
        var meteringPoint = new MeteringPoint("123456789012345678");
        const string aggregateGroup = "TestGroup";
        const Resolution resolution = Resolution.Hourly;

        // Act
        var key = new AggregationGroupCompositeKey(meteringPoint, aggregateGroup, resolution);

        // Assert
        Assert.Equal($"{meteringPoint.Id}_{aggregateGroup}_{resolution}", key.Key);
    }
}

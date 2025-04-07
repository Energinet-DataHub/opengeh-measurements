using Energinet.DataHub.Measurements.Application.Requests;
using Xunit;

namespace Energinet.DataHub.Measurements.UnitTests.Application.Requests;

public class GetAggregatedMeasurementsForMonthRequestTests
{
    [Fact]
    public void GetAggregatedMeasurementsForMonthRequest_WhenValid_ReturnsExpected()
    {
        // Arrange
        const string meteringPointId = "123456789";
        const int year = 2025;
        const int month = 1;

        // Act
        var actual = new GetAggregatedMeasurementsForMonthRequest(meteringPointId, year, month);

        // Assert
        Assert.Equal(meteringPointId, actual.MeteringPointId);
        Assert.Equal(year, actual.Year);
        Assert.Equal(month, actual.Month);
    }

    [Theory]
    [InlineData(2025, 13)]
    [InlineData(2025, 0)]
    public void GetAggregatedMeasurementsForMonthRequest_WhenInvalidMonth_ThrowsArgumentOutOfRangeException(int year, int month)
    {
        // Arrange
        const string meteringPointId = "123456789";

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => new GetAggregatedMeasurementsForMonthRequest(meteringPointId, year, month));
    }
}

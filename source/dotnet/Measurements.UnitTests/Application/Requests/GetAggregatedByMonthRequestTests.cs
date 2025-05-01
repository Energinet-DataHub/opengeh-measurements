using Energinet.DataHub.Measurements.Application.Requests;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Application.Requests;

[UnitTest]
public class GetAggregatedByMonthRequestTests
{
    [Fact]
    public void GetAggregatedByMonthRequest_WhenValid_ReturnsExpected()
    {
        // Arrange
        const string meteringPointId = "123456789";
        const int year = 2025;

        // Act
        var actual = new GetAggregatedByMonthRequest(meteringPointId, year);

        // Assert
        Assert.Equal(meteringPointId, actual.MeteringPointId);
        Assert.Equal(year, actual.Year);
    }
}

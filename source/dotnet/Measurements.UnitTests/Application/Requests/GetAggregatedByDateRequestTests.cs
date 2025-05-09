﻿using Energinet.DataHub.Measurements.Application.Requests;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Application.Requests;

[UnitTest]
public class GetAggregatedByDateRequestTests
{
    [Fact]
    public void GetAggregatedByMonthRequest_WhenValid_ReturnsExpected()
    {
        // Arrange
        const string meteringPointId = "123456789";
        const int year = 2025;
        const int month = 1;

        // Act
        var actual = new GetAggregatedByDateRequest(meteringPointId, year, month);

        // Assert
        Assert.Equal(meteringPointId, actual.MeteringPointId);
        Assert.Equal(year, actual.Year);
        Assert.Equal(month, actual.Month);
    }
}

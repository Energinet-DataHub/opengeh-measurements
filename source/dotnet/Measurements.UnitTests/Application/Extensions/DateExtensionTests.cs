﻿using System.Globalization;
using Energinet.DataHub.Measurements.Application.Extensions;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Application.Extensions;

[UnitTest]
public class InstantExtensionTests
{
    [Theory]
    [InlineData(2025, 3, 29, 23,  "2025-03-30")]
    [InlineData(2025, 3, 30, 22, "2025-03-31")]
    [InlineData(2025, 3, 31, 22, "2025-04-01")]
    [InlineData(2025, 10, 24, 22, "2025-10-25")]
    [InlineData(2025, 10, 25, 22, "2025-10-26")]
    [InlineData(2025, 10, 26, 23, "2025-10-27")]
    public void ToDateOnly_WhenCalled_ReturnsLocalDate(int year, int month, int day, int hour, string expected)
    {
        // Arrange
        var date = Instant.FromUtc(year, month, day, hour, 0, 0);

        // Act
        var actual = date.ToDateOnly();

        // Assert
        Assert.Equal(expected, actual.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
    }
}

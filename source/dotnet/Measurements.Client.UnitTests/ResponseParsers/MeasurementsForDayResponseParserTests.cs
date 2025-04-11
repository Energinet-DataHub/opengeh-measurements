using System.Net;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Client.ResponseParsers;
using Energinet.DataHub.Measurements.Client.UnitTests.Assets;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.Client.UnitTests.ResponseParsers;

[UnitTest]
public class MeasurementsForDayResponseParserTests
{
    [Fact]
    public async Task ParseResponseMessage_WhenCalledWithValidResponse_ReturnsMeasurementDto()
    {
        // Arrange
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsForSingleDay);
        var sut = new MeasurementsForDayResponseParser();

        // Act
        var actual = await sut.ParseResponseMessage(response, CancellationToken.None);

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(24, actual.MeasurementPositions.Count());
    }

    [Fact]
    public async Task ParseResponseMessage_WhenResponseContainsHistoricalValues_PositionIndexAreOrdered()
    {
        // Arrange
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsForDayWithHistoricalObservations);
        var sut = new MeasurementsForDayResponseParser();

        // Act
        var actual = await sut.ParseResponseMessage(response, CancellationToken.None);
        var firstPosition = actual.MeasurementPositions.Single(p => p.ObservationTime.ToFormattedString().Equals("2025-01-01T23:00:00Z"));
        var secondPosition = actual.MeasurementPositions.Single(p => p.ObservationTime.ToFormattedString().Equals("2025-01-02T00:00:00Z"));

        // Assert
        Assert.Equal(1, firstPosition.Index);
        Assert.Equal(2, secondPosition.Index);
        Assert.NotNull(actual);
    }

    [Fact]
    public async Task ParseResponseMessage_WhenResponseContainsHistoricalValues_PointAreOrderedCorrect()
    {
        // Arrange
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsForDayWithHistoricalObservations);
        var sut = new MeasurementsForDayResponseParser();

        // Act
        var actual = await sut.ParseResponseMessage(response, CancellationToken.None);
        var firstPosition = actual.MeasurementPositions.Single(p => p.Index == 1);
        var actualCurrentQuantityOfFirstPosition = firstPosition.MeasurementPoints.Single(p => p.Order.Equals(1)).Quantity;

        // Assert
        Assert.Equal(3, firstPosition.MeasurementPoints.Count());
        Assert.Equal(20.4M, actualCurrentQuantityOfFirstPosition);
        Assert.NotNull(actual);
    }

    private static HttpResponseMessage CreateResponse(HttpStatusCode statusCode, string content)
    {
        return new HttpResponseMessage
        {
            StatusCode = statusCode,
            Content = new StringContent(content),
        };
    }
}

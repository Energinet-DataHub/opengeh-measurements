using System.Net;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
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
        var actualPosition = actual.MeasurementPositions.First();
        var actualPoint = actualPosition.MeasurementPoints.First();

        // Assert positions
        Assert.NotNull(actual);
        Assert.Equal(24, actual.MeasurementPositions.Count());

        // Assert first position
        Assert.Equal("2025-01-01T23:00:00Z", actualPosition.ObservationTime.ToFormattedString());
        Assert.Single(actualPosition.MeasurementPoints);
        Assert.Equal(1, actualPosition.Index);

        // Assert first point
        Assert.Equal(1, actualPoint.Order);
        Assert.Equal(1.4m, actualPoint.Quantity);
        Assert.Equal(Quality.Measured, actualPoint.Quality);
        Assert.Equal(Unit.kWh, actualPoint.Unit);
        Assert.Equal(Resolution.Hourly, actualPoint.Resolution);
        Assert.Equal("2025-01-17T03:40:55Z", actualPoint.PersistedTime.ToFormattedString());
        Assert.Equal("2025-01-15T03:40:55Z", actualPoint.RegistrationTime.ToFormattedString());
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
    public async Task ParseResponseMessage_WhenResponseContainsHistoricalValues_PointsAreOrderedCorrectly()
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

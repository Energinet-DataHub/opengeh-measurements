using System.Net;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Client.ResponseParsers;
using Energinet.DataHub.Measurements.Client.UnitTests.Assets;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.Client.UnitTests.ResponseParsers;

[UnitTest]
public class MeasurementsForPeriodResponseParserTests
{
    [Fact]
    public async Task ParseResponseMessage_WhenCalledWithValidResponse_ReturnsMeasurementDto()
    {
        // Arrange
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsForSingleDay);
        var sut = new MeasurementsForPeriodResponseParser();

        // Act
        var actual = await sut.ParseResponseMessage(response, CancellationToken.None);
        var actualPosition = actual.First();

        // Assert
        Assert.Equal(24, actual.Count);
        Assert.All(actual, point =>
        {
            Assert.Equal(Quality.Measured, point.Quality);
            Assert.Equal(Resolution.Hourly, point.Resolution);
            Assert.Equal(Unit.kWh, point.Unit);
        });

        // Assert first
        var actualFirstPoint = actual.First();
        Assert.Equal(1, actualPosition.Order);
        Assert.Equal(1.4m, actualFirstPoint.Quantity);
        Assert.Equal("2025-01-17T03:40:55Z", actualFirstPoint.PersistedTime.ToFormattedString());
        Assert.Equal("2025-01-15T03:40:55Z", actualFirstPoint.RegistrationTime.ToFormattedString());
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

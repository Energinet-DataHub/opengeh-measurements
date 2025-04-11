using System.Net;
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

    private static HttpResponseMessage CreateResponse(HttpStatusCode statusCode, string content)
    {
        return new HttpResponseMessage
        {
            StatusCode = statusCode,
            Content = new StringContent(content),
        };
    }
}

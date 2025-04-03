using System.Net;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.UnitTests.Assets;
using Moq;
using Moq.Protected;
using NodaTime;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.Client.UnitTests;

[UnitTest]
public class MeasurementsClientTests
{
    [Theory]
    [InlineData(2023, 1, 2)]
    [InlineData(2023, 6, 15)]
    public async Task GetMeasurementsForDayAsync_WhenCalledWithValidQuery_ReturnsListOfPoints(
        int year, int month, int day)
    {
        // Arrange
        var query = new GetMeasurementsForDayQuery("1234567890", new LocalDate(year, month, day));
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsForDayExcludingHistory);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object);

        // Act
        var actual = (await sut.GetMeasurementsForDayAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(24, actual.Count);
        Assert.True(actual.All(p => p.Quality == Quality.Measured));
    }

    [Fact]
    public async Task GetMeasurementsForDayAsync_WhenCalledWithQueryWithNoMeasurements_ReturnsEmptyList()
    {
        // Arrange
        var query = new GetMeasurementsForDayQuery(
            "1234567890",
            new LocalDate(1990, 1, 2));
        var response = CreateResponse(HttpStatusCode.NotFound, string.Empty);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object);

        // Act
        var actual = (await sut.GetMeasurementsForDayAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.Empty(actual);
    }

    [Fact]
    public async Task GetMeasurementsForPeriodAsync_WhenCalledWithValidQuery_ReturnsListOfPoints()
    {
        // Arrange
        var from = new LocalDate(2023, 1, 2);
        var to = new LocalDate(2023, 1, 6);
        var query = new GetMeasurementsForPeriodQuery("1234567890", from, to);
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsForMultipleDays);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object);

        // Act
        var actual = (await sut.GetMeasurementsForPeriodAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(96, actual.Count);
        Assert.True(actual.All(p => p.Quality == Quality.Measured));
    }

    [Fact]
    public async Task GetAggregatedMeasurementsForDayAsync_WhenCalledWithValidQuery_ReturnsListOfMeasurementAggregations()
    {
        // Arrange
        var query = new GetAggregatedMeasurementsForMonthQuery("1234567890", new YearMonth(2025, 3));
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsAggregatedByDay);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object);

        // Act
        var actual = (await sut.GetAggregatedMeasurementsForMonth(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(31, actual.Count);
        Assert.True(actual.First().Date == new LocalDate(2025, 3, 1));
        Assert.True(actual.Last().Date == new LocalDate(2025, 3, 31));
        Assert.True(actual.All(p => p.MissingValues));
    }

    private static Mock<IHttpClientFactory> CreateHttpClientFactoryMock(HttpClient httpClient)
    {
        var httpClientFactoryMock = new Mock<IHttpClientFactory>();
        httpClientFactoryMock
            .Setup(x => x.CreateClient(It.IsAny<string>()))
            .Returns(httpClient);
        return httpClientFactoryMock;
    }

    private static HttpClient CreateHttpClient(HttpResponseMessage response)
    {
        var httpMessageHandlerMock = new Mock<HttpMessageHandler>(MockBehavior.Strict);

        httpMessageHandlerMock
            .Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(response);

        return new HttpClient(httpMessageHandlerMock.Object) { BaseAddress = new Uri("http://localhost") };
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

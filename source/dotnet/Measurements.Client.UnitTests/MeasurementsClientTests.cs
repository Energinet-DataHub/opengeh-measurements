using System.Net;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Client.Factories;
using Energinet.DataHub.Measurements.Client.UnitTests.Assets;
using Moq;
using Moq.Protected;
using NodaTime;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.Client.UnitTests;

[UnitTest]
public class MeasurementsClientTests
{
    [Fact]
    public async Task GetMeasurementsForDayAsync_WhenCalledWithValidQuery_ReturnsListOfPoints()
    {
        // Arrange
        var query = new GetMeasurementsForDayQuery("1234567890", new LocalDate(1, 2, 3));
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsForSingleDay);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object, new MeasurementAggregationFactory());

        // Act
        var actual = (await sut.GetMeasurementsForDayAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(24, actual.Count);
        Assert.True(actual.All(p => p.Quality == Quality.Measured));
        Assert.True(actual.All(p => p.Created.ToFormattedString() == "2022-01-03T15:00:00Z"));
    }

    [Fact]
    public async Task GetMeasurementsForDayAsync_WhenCalledWithQueryWithNoMeasurements_ReturnsEmptyList()
    {
        // Arrange
        var query = new GetMeasurementsForDayQuery("1234567890", new LocalDate(1, 2, 3));
        var response = CreateResponse(HttpStatusCode.NotFound, string.Empty);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object, new MeasurementAggregationFactory());

        // Act
        var actual = (await sut.GetMeasurementsForDayAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.Empty(actual);
    }

    [Fact]
    public async Task GetMeasurementsForPeriodAsync_WhenCalledWithValidQuery_ReturnsListOfPoints()
    {
        // Arrange
        var testDate = new LocalDate(1, 2, 3);
        var query = new GetMeasurementsForPeriodQuery("1234567890", testDate, testDate);
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsForMultipleDays);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object, new MeasurementAggregationFactory());

        // Act
        var actual = (await sut.GetMeasurementsForPeriodAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(96, actual.Count);
        Assert.True(actual.All(p => p.Quality == Quality.Measured));
    }

    [Fact]
    public async Task GetAggregatedMeasurementsForDayAsync_WhenCalledForHourlyMeasuredMeteringPoint_ReturnsListOfMeasurementAggregations()
    {
        // Arrange
        var query = new GetAggregatedMeasurementsForMonthQuery("1234567890", new YearMonth(2025, 3));
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.HourlyMeasurementsAggregatedByDay);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object, new MeasurementAggregationFactory());

        // Act
        var actual = (await sut.GetAggregatedMeasurementsForMonth(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(31, actual.Count);
        Assert.True(actual.First().Date == new LocalDate(2025, 3, 1));
        Assert.True(actual.Last().Date == new LocalDate(2025, 3, 31));
        Assert.False(actual.All(p => p.MissingValues));
        Assert.True(actual.All(p => p.Quality == Quality.Measured));
    }

    [Fact]
    public async Task GetAggregatedMeasurementsForDayAsync_WhenCalledForQuarterlyMeasuredMeteringPoint_ReturnsListOfMeasurementAggregations()
    {
        // Arrange
        var query = new GetAggregatedMeasurementsForMonthQuery("1234567890", new YearMonth(2025, 3));
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.QuarterlyMeasurementsAggregatedByDay);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object, new MeasurementAggregationFactory());

        // Act
        var actual = (await sut.GetAggregatedMeasurementsForMonth(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(31, actual.Count);
        Assert.True(actual.First().Date == new LocalDate(2025, 3, 1));
        Assert.True(actual.Last().Date == new LocalDate(2025, 3, 31));
        Assert.False(actual.All(p => p.MissingValues));
        Assert.True(actual.All(p => p.Quality == Quality.Measured));
    }

    [Fact]
    public async Task GetAggregatedMeasurementsForDayAsync_WhenCalledDataIsMissing_ReturnsCompleteListOfMeasurementAggregations()
    {
        // Arrange
        var query = new GetAggregatedMeasurementsForMonthQuery("1234567890", new YearMonth(2024, 10));
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.HourlyMeasurementsAggregatedByDayMissingMeasurements);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object, new MeasurementAggregationFactory());

        // Act
        var actual = (await sut.GetAggregatedMeasurementsForMonth(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(30, actual.Count);
        Assert.True(actual.First().Date == new LocalDate(2024, 10, 1));
        Assert.True(actual.Last().Date == new LocalDate(2024, 10, 31));
        Assert.True(actual.Last().MissingValues);
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

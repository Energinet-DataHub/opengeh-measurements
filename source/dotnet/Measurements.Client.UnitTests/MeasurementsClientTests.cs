using System.Net;
using Energinet.DataHub.Core.TestCommon.AutoFixture.Attributes;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.ResponseParsers;
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
    [AutoMoqData]
    public async Task GetByDayAsync_WhenCalledWithValidQuery_ReturnsMeasurement(
        Mock<IMeasurementsForDayResponseParser> measurementsForDayResponseParser,
        MeasurementDto expectedMeasurementDto)
    {
        // Arrange
        var query = new GetByDayQuery("1234567890123", new LocalDate(1, 2, 3));
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsForSingleDay);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        measurementsForDayResponseParser
            .Setup(x => x.ParseResponseMessage(response, CancellationToken.None))
            .ReturnsAsync(expectedMeasurementDto);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object, measurementsForDayResponseParser.Object);

        // Act
        var actual = await sut.GetByDayAsync(query, CancellationToken.None);

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(expectedMeasurementDto, actual);
    }

    [Theory]
    [AutoMoqData]
    public async Task GetByDayAsync_WhenCalledWithQueryWithNoMeasurements_ReturnsEmptyList(
        Mock<IMeasurementsForDayResponseParser> measurementsForDayResponseParser)
    {
        // Arrange
        var query = new GetByDayQuery("1234567890123", new LocalDate(1, 2, 3));
        var response = CreateResponse(HttpStatusCode.NotFound, string.Empty);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object, measurementsForDayResponseParser.Object);

        // Act
        var actual = (await sut.GetByDayAsync(query, CancellationToken.None)).MeasurementPositions;

        // Assert
        Assert.Empty(actual);
    }

    [Theory]
    [AutoMoqData]
    public async Task GetAggregatedByMonth_WhenCalled_ReturnsListOfMeasurementAggregations(
        Mock<IMeasurementsForDayResponseParser> measurementsForDayResponseParser)
    {
        // Arrange
        var query = new GetMonthlyAggregateByDateQuery("1234567890123", new YearMonth(2025, 3));
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsAggregatedByDate);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object, measurementsForDayResponseParser.Object);

        // Act
        var actual = (await sut.GetMonthlyAggregateByDateAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(31, actual.Count);
        Assert.True(actual.First().Date == new DateOnly(2025, 3, 1));
        Assert.True(actual.Last().Date == new DateOnly(2025, 3, 31));
        Assert.False(actual.All(p => p.MissingValues));
        Assert.True(actual.All(p => p.Quality == Quality.Measured));
        Assert.True(actual.All(p => p.Unit == Unit.kWh));
        Assert.False(actual.All(p => p.ContainsUpdatedValues));
    }

    [Theory]
    [AutoMoqData]
    public async Task GetAggregatedMeasurementsByDateAsync_WhenCalledDataIsMissing_ReturnsCompleteListOfMeasurementAggregations(
        Mock<IMeasurementsForDayResponseParser> measurementsForDayResponseParser)
    {
        // Arrange
        var query = new GetMonthlyAggregateByDateQuery("1234567890123", new YearMonth(2024, 10));
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsAggregatedByDateMissingMeasurements);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object, measurementsForDayResponseParser.Object);

        // Act
        var actual = (await sut.GetMonthlyAggregateByDateAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(30, actual.Count);
        Assert.True(actual.First().Date == new DateOnly(2024, 10, 1));
        Assert.True(actual.Last().Date == new DateOnly(2024, 10, 31));
        Assert.True(actual.Last().MissingValues);
    }

    [Theory]
    [AutoMoqData]
    public async Task GetAggregatedByMonth_WhenCalledForMeasuredMeteringPoint_ReturnsListOfMeasurementAggregations(
        Mock<IMeasurementsForDayResponseParser> measurementsForDayResponseParser)
    {
        // Arrange
        var query = new GetYearlyAggregateByMonthQuery("1234567890123", 2025);
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsAggregatedByMonth);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object, measurementsForDayResponseParser.Object);

        // Act
        var actual = (await sut.GetYearlyAggregateByMonthAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(12, actual.Count);
        Assert.True(actual.First().YearMonth == new YearMonth(2025, 1));
        Assert.True(actual.Last().YearMonth == new YearMonth(2025, 12));
        Assert.True(actual.All(p => p.Quality == Quality.Measured));
        Assert.True(actual.All(p => p.Unit == Unit.kWh));
    }

    [Theory]
    [AutoMoqData]
    public async Task GetAggregatedByYear_WhenCalledForMeasuredMeteringPoint_ReturnsListOfMeasurementAggregations(
        Mock<IMeasurementsForDayResponseParser> measurementsForDayResponseParser)
    {
        // Arrange
        var query = new GetAggregateByYearQuery("1234567890");
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsAggregatedByYear);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(httpClientFactoryMock.Object, measurementsForDayResponseParser.Object);

        // Act
        var actual = (await sut.GetAggregateByYearAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(5, actual.Count);
        Assert.Equal(2021, actual.First().Year);
        Assert.Equal(2025, actual.Last().Year);
        Assert.Equal(Quality.Measured, actual.First().Quality);
        Assert.Equal(Quality.Estimated, actual.Last().Quality);
        Assert.True(actual.All(p => p.Unit == Unit.kWh));
    }

    [Fact]
    public async Task GetAggregateByPeriodAsync_WhenCalled_ThrowsNotImplementedException()
    {
        // Arrange
        var query = new GetAggregateByPeriodQuery(["1234567890"], Instant.MinValue, Instant.MaxValue, Aggregation.Quarter);
        var httpClientFactoryMock = new Mock<IHttpClientFactory>();
        var sut = new MeasurementsClient(httpClientFactoryMock.Object, Mock.Of<IMeasurementsForDayResponseParser>());

        // Act & Assert
        await Assert.ThrowsAsync<NotImplementedException>(() => sut.GetAggregateByPeriodAsync(query, CancellationToken.None));
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

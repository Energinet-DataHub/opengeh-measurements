using System.Collections.ObjectModel;
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
        Mock<IMeasurementsForDateResponseParser> measurementsForDayResponseParser,
        Mock<IMeasurementsForPeriodResponseParser> measurementsForPeriodResponseParser,
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
        var sut = new MeasurementsClient(
            httpClientFactoryMock.Object, measurementsForDayResponseParser.Object, measurementsForPeriodResponseParser.Object);

        // Act
        var actual = await sut.GetByDayAsync(query, CancellationToken.None);

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(expectedMeasurementDto, actual);
    }

    [Theory]
    [AutoMoqData]
    public async Task GetByDayAsync_WhenCalledWithQueryWithNoMeasurements_ReturnsEmptyList(
        Mock<IMeasurementsForDateResponseParser> measurementsForDayResponseParser,
        Mock<IMeasurementsForPeriodResponseParser> measurementsForPeriodResponseParser)
    {
        // Arrange
        var query = new GetByDayQuery("1234567890123", new LocalDate(1, 2, 3));
        var response = CreateResponse(HttpStatusCode.NotFound, string.Empty);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(
            httpClientFactoryMock.Object, measurementsForDayResponseParser.Object, measurementsForPeriodResponseParser.Object);

        // Act
        var actual = (await sut.GetByDayAsync(query, CancellationToken.None)).MeasurementPositions;

        // Assert
        Assert.Empty(actual);
    }

    [Theory]
    [AutoMoqData]
    public async Task GetCurrentByPeriodAsync_WhenCalledWithValidQuery_ReturnsMeasurement(
        Mock<IMeasurementsForDateResponseParser> measurementsForDayResponseParser,
        Mock<IMeasurementsForPeriodResponseParser> measurementsForPeriodResponseParser,
        ReadOnlyCollection<MeasurementPointDto> expectedMeasurementPoints)
    {
        // Arrange
        var from = Instant.FromDateTimeOffset(new DateTimeOffset(2025, 1, 1, 23, 0, 0, TimeSpan.FromHours(1)));
        var to = Instant.FromDateTimeOffset(new DateTimeOffset(2025, 1, 2, 23, 0, 0, TimeSpan.FromHours(1)));
        var query = new GetByPeriodQuery("1234567890123", from, to);
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsForSingleDay);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        measurementsForPeriodResponseParser
            .Setup(x => x.ParseResponseMessage(response, CancellationToken.None))
            .ReturnsAsync(expectedMeasurementPoints);
        var sut = new MeasurementsClient(
            httpClientFactoryMock.Object, measurementsForDayResponseParser.Object, measurementsForPeriodResponseParser.Object);

        // Act
        var actual = await sut.GetCurrentByPeriodAsync(query, CancellationToken.None);

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(expectedMeasurementPoints, actual);
    }

    [Theory]
    [AutoMoqData]
    public async Task GetCurrentByPeriodAsync_WhenCalledWithQueryWithNoMeasurements_ReturnsEmptyList(
        Mock<IMeasurementsForDateResponseParser> measurementsForDayResponseParser,
        Mock<IMeasurementsForPeriodResponseParser> measurementsForPeriodResponseParser)
    {
        // Arrange
        var from = Instant.FromDateTimeOffset(new DateTimeOffset(2025, 1, 1, 23, 0, 0, TimeSpan.FromHours(1)));
        var to = Instant.FromDateTimeOffset(new DateTimeOffset(2025, 1, 2, 23, 0, 0, TimeSpan.FromHours(1)));
        var query = new GetByPeriodQuery("1234567890123", from, to);
        var response = CreateResponse(HttpStatusCode.NotFound, string.Empty);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(
            httpClientFactoryMock.Object, measurementsForDayResponseParser.Object, measurementsForPeriodResponseParser.Object);

        // Act
        var actual = await sut.GetCurrentByPeriodAsync(query, CancellationToken.None);

        // Assert
        Assert.Empty(actual);
    }

    [Theory]
    [AutoMoqData]
    public async Task GetAggregatedByMonth_WhenCalled_ReturnsListOfMeasurementAggregations(
        Mock<IMeasurementsForDateResponseParser> measurementsForDayResponseParser,
        Mock<IMeasurementsForPeriodResponseParser> measurementsForPeriodResponseParser)
    {
        // Arrange
        var query = new GetMonthlyAggregateByDateQuery("1234567890123", new YearMonth(2025, 3));
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsAggregatedByDate);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(
            httpClientFactoryMock.Object, measurementsForDayResponseParser.Object, measurementsForPeriodResponseParser.Object);

        // Act
        var actual = (await sut.GetMonthlyAggregateByDateAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(31, actual.Count);
        Assert.True(actual.First().Date == new DateOnly(2025, 3, 1));
        Assert.True(actual.Last().Date == new DateOnly(2025, 3, 31));
        Assert.All(actual, p => Assert.False(p.IsMissingValues));
        Assert.All(actual, p => Assert.Contains(Quality.Measured, p.Qualities));
        Assert.All(actual, p => Assert.Equal(Unit.kWh, p.Unit));
        Assert.All(actual, p => Assert.False(p.ContainsUpdatedValues));
    }

    [Theory]
    [AutoMoqData]
    public async Task GetAggregatedMeasurementsByDateAsync_WhenCalledDataIsMissing_ReturnsCompleteListOfMeasurementAggregations(
        Mock<IMeasurementsForDateResponseParser> measurementsForDayResponseParser,
        Mock<IMeasurementsForPeriodResponseParser> measurementsForPeriodResponseParser)
    {
        // Arrange
        var query = new GetMonthlyAggregateByDateQuery("1234567890123", new YearMonth(2024, 10));
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsAggregatedByDateMissingMeasurements);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(
            httpClientFactoryMock.Object, measurementsForDayResponseParser.Object, measurementsForPeriodResponseParser.Object);

        // Act
        var actual = (await sut.GetMonthlyAggregateByDateAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(30, actual.Count);
        Assert.True(actual.First().Date == new DateOnly(2024, 10, 1));
        Assert.True(actual.Last().Date == new DateOnly(2024, 10, 31));
        Assert.True(actual.Last().IsMissingValues);
    }

    [Theory]
    [AutoMoqData]
    public async Task GetAggregatedByMonth_WhenCalledForMeasuredMeteringPoint_ReturnsListOfMeasurementAggregations(
        Mock<IMeasurementsForDateResponseParser> measurementsForDayResponseParser,
        Mock<IMeasurementsForPeriodResponseParser> measurementsForPeriodResponseParser)
    {
        // Arrange
        var query = new GetYearlyAggregateByMonthQuery("1234567890123", 2025);
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsAggregatedByMonth);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(
            httpClientFactoryMock.Object, measurementsForDayResponseParser.Object, measurementsForPeriodResponseParser.Object);

        // Act
        var actual = (await sut.GetYearlyAggregateByMonthAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(12, actual.Count);
        Assert.True(actual.First().YearMonth == new YearMonth(2025, 1));
        Assert.True(actual.Last().YearMonth == new YearMonth(2025, 12));
        Assert.All(actual, p => Assert.Equal(Unit.kWh, p.Unit));
    }

    [Theory]
    [AutoMoqData]
    public async Task GetAggregatedByYear_WhenCalledForMeasuredMeteringPoint_ReturnsListOfMeasurementAggregations(
        Mock<IMeasurementsForDateResponseParser> measurementsForDayResponseParser,
        Mock<IMeasurementsForPeriodResponseParser> measurementsForPeriodResponseParser)
    {
        // Arrange
        var query = new GetAggregateByYearQuery("1234567890");
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsAggregatedByYear);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(
            httpClientFactoryMock.Object, measurementsForDayResponseParser.Object, measurementsForPeriodResponseParser.Object);

        // Act
        var actual = (await sut.GetAggregateByYearAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(5, actual.Count);
        Assert.Equal(2021, actual.First().Year);
        Assert.Equal(2025, actual.Last().Year);
        Assert.All(actual, p => Assert.Equal(Unit.kWh, p.Unit));
    }

    [Theory]
    [AutoMoqData]
    public async Task GetAggregatedByPeriod_WhenCalledForMeasuredMeteringPoint_ReturnsListOfMeasurementAggregations(
        Mock<IMeasurementsForDateResponseParser> measurementsForDayResponseParser,
        Mock<IMeasurementsForPeriodResponseParser> measurementsForPeriodResponseParser)
    {
        // Arrange
        var from = Instant.FromDateTimeOffset(new DateTimeOffset(2025, 3, 1, 0, 0, 0, TimeSpan.FromHours(1)));
        var to = Instant.FromDateTimeOffset(new DateTimeOffset(2025, 4, 1, 0, 0, 0, TimeSpan.FromHours(2)));
        var query = new GetAggregateByPeriodQuery(["123456789123456789"], from, to, Aggregation.Day);
        var response = CreateResponse(HttpStatusCode.OK, TestAssets.MeasurementsAggregatedByPeriod);
        var httpClient = CreateHttpClient(response);
        var httpClientFactoryMock = CreateHttpClientFactoryMock(httpClient);
        var sut = new MeasurementsClient(
            httpClientFactoryMock.Object, measurementsForDayResponseParser.Object, measurementsForPeriodResponseParser.Object);

        // Act
        var actual = (await sut.GetAggregatedByPeriodAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Single(actual);
        var actualFirst = actual.First();
        Assert.Equal("123456789123456789", actualFirst.MeteringPoint.Id);
        Assert.Single(actualFirst.PointAggregationGroups);
        Assert.Equal(Resolution.Hourly, actualFirst.PointAggregationGroups.First().Value.Resolution);
        Assert.Equal(from, actualFirst.PointAggregationGroups.First().Value.From);
        Assert.Equal(to, actualFirst.PointAggregationGroups.First().Value.To);
        Assert.Equal(31, actualFirst.PointAggregationGroups.First().Value.PointAggregations.Count);
        Assert.All(actualFirst.PointAggregationGroups.First().Value.PointAggregations, p =>
        {
            Assert.Equal(Quality.Measured, p.Quality);
            Assert.Equal(1, p.Quantity);
        });
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

using System.Globalization;
using System.Net;
using Energinet.DataHub.Measurements.Application.Extensions;
using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Controllers;

[IntegrationTest]
public class MeasurementsControllerTests(WebApiFixture fixture) : IClassFixture<WebApiFixture>
{
    [Fact]
    public async Task GetByPeriodAsync_WhenMeteringPointExists_ReturnsValidMeasurements()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        const string startDate = "2022-01-01T23:00:00Z";
        const string endDate = "2022-01-02T23:00:00Z";
        var url = CreateGetMeasurementsForPeriodUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<MeasurementsResponse>(actualResponse);

        // Assert
        Assert.Equal(24, actual.Points.Count);
        foreach (var point in actual.Points)
        {
            Assert.Equal(Unit.kWh, point.Unit);
            Assert.Equal(Quality.Measured, point.Quality);
            Assert.Equal(Resolution.Hourly, point.Resolution);
            Assert.Equal("2022-01-04T23:00:00Z", point.Created.ToString("yyyy-MM-ddTHH:mm:ss'Z'", CultureInfo.InvariantCulture));
        }
    }

    [Fact]
    public async Task GetByPeriodAsync_WhenMultipleObservations_ReturnsAllObservations()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        const string startDate = "2022-01-02T23:00:00Z"; // On this date, the fixture inserts 2 measurements with the same observation time
        const string endDate = "2022-01-03T23:00:00Z";
        var url = CreateGetMeasurementsForPeriodUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<MeasurementsResponse>(actualResponse);

        // Assert
        Assert.Equal(2, actual.Points.Count(p => p.ObservationTime.ToString().Equals(startDate)));
    }

    [Fact]
    public async Task GetByPeriodAsync_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
    {
        // Arrange
        const string expectedMeteringPointId = "not existing id";
        const string startDate = "2022-01-31T23:00:00Z";
        const string endDate = "2022-01-01T23:00:00Z";
        var url = CreateGetMeasurementsForPeriodUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetByPeriodAsync_WhenMeasurementHasInvalidUnit_ReturnInternalServerError()
    {
        // Arrange
        const string expectedMeteringPointId = "9876543210";
        const string startDate = "2022-06-14T22:00:00Z"; // On this date, the fixture inserts a measurement with invalid unit
        const string endDate = "2022-06-15T22:00:00Z";
        var url = CreateGetMeasurementsForPeriodUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actual = await fixture.Client.GetAsync(url);
        var actualContent = await actual.Content.ReadAsStringAsync();

        // Assert
        Assert.Equal(HttpStatusCode.InternalServerError, actual.StatusCode);
        Assert.Contains("An unknown error occured while handling request to the Measurements API. Try again later.", actualContent);
    }

    [Fact]
    public async Task GetByPeriodAsync_WhenMeasurementHasInvalidResolution_ReturnInternalServerError()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        const string startDate = "2022-01-31T23:00:00Z"; // On this date, the fixture inserts a measurement with invalid resolution
        const string endDate = "2022-02-01T23:00:00Z";
        var url = CreateGetMeasurementsForPeriodUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actual = await fixture.Client.GetAsync(url);
        var actualContent = await actual.Content.ReadAsStringAsync();

        // Assert
        Assert.Equal(HttpStatusCode.InternalServerError, actual.StatusCode);
        Assert.Contains("An unknown error occured while handling request to the Measurements API. Try again later.", actualContent);
    }

    [Fact]
    public async Task GetAggregatedByMonthAsyncV2_WhenMeteringPointExists_ReturnsValidAggregatedMeasurements()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        var expectedDate = Instant.FromUtc(2021, 2, 1, 23, 0, 0).ToDateOnly();
        var yearMonth = new YearMonth(2021, 2);
        var url = CreateGetAggregatedMeasurementsByMonthV2Url(expectedMeteringPointId, yearMonth);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<MeasurementsAggregatedByDateResponse>(actualResponse);

        // Assert
        Assert.Equal(HttpStatusCode.OK, actualResponse.StatusCode);
        Assert.Equal(2, actual.MeasurementAggregations.Count);
        Assert.Equal(expectedDate, actual.MeasurementAggregations.First().Date);
        foreach (var measurementAggregation in actual.MeasurementAggregations)
        {
            Assert.Equal(Quality.Measured, measurementAggregation.Quality);
            Assert.Equal(285.6M, measurementAggregation.Quantity);
            Assert.False(measurementAggregation.MissingValues);
            Assert.Equal(Unit.kWh, measurementAggregation.Unit);
            Assert.False(measurementAggregation.ContainsUpdatedValues);
        }
    }

    [Fact]
    public async Task GetAggregatedByMonthAsyncV2_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
    {
        // Arrange
        const string expectedMeteringPointId = "not existing id";
        var yearMonth = new YearMonth(2022, 1);
        var url = CreateGetAggregatedMeasurementsByMonthV2Url(expectedMeteringPointId, yearMonth);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetAggregatedByYearAsyncV2_WhenMeteringPointExists_ReturnsValidAggregatedMeasurements()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        var expectedDate = Instant.FromUtc(2021, 2, 1, 23, 0, 0).ToDateOnly();
        var expectedYearMonth = new YearMonth(expectedDate.Year, expectedDate.Month);
        var url = CreateGetAggregatedMeasurementsByYearV2Url(expectedMeteringPointId, new Year(expectedYearMonth.Year));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<MeasurementsAggregatedByMonthResponse>(actualResponse);

        // Assert
        Assert.Equal(HttpStatusCode.OK, actualResponse.StatusCode);
        Assert.Single(actual.MeasurementAggregations);
        Assert.Equal(expectedYearMonth, actual.MeasurementAggregations.First().YearMonth);
        foreach (var measurementAggregation in actual.MeasurementAggregations)
        {
            Assert.Equal(Quality.Measured, measurementAggregation.Quality);
            Assert.Equal(571.2M, measurementAggregation.Quantity);
            Assert.Equal(Unit.kWh, measurementAggregation.Unit);
        }
    }

    [Fact]
    public async Task GetAggregatedByYearAsyncV2_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
    {
        // Arrange
        const string expectedMeteringPointId = "not existing id";
        var yearMonth = new YearMonth(2022, 1);
        var url = CreateGetAggregatedMeasurementsByYearV2Url(expectedMeteringPointId, new Year(yearMonth.Year));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetAggregatedByDateAsync_WhenMeteringPointExists_ReturnsValidAggregatedMeasurements()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        var expectedDate = Instant.FromUtc(2021, 2, 1, 23, 0, 0).ToDateOnly();
        var yearMonth = new YearMonth(2021, 2);
        var url = CreateGetAggregatedMeasurementsByDateUrl(expectedMeteringPointId, yearMonth);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<MeasurementsAggregatedByDateResponse>(actualResponse);

        // Assert
        Assert.Equal(HttpStatusCode.OK, actualResponse.StatusCode);
        Assert.Equal(2, actual.MeasurementAggregations.Count);
        Assert.Equal(expectedDate, actual.MeasurementAggregations.First().Date);
        foreach (var measurementAggregation in actual.MeasurementAggregations)
        {
            Assert.Equal(Quality.Measured, measurementAggregation.Quality);
            Assert.Equal(285.6M, measurementAggregation.Quantity);
            Assert.False(measurementAggregation.MissingValues);
            Assert.Equal(Unit.kWh, measurementAggregation.Unit);
            Assert.False(measurementAggregation.ContainsUpdatedValues);
        }
    }

    [Fact]
    public async Task GetAggregatedByDateAsync_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
    {
        // Arrange
        const string expectedMeteringPointId = "not existing id";
        var yearMonth = new YearMonth(2022, 1);
        var url = CreateGetAggregatedMeasurementsByDateUrl(expectedMeteringPointId, yearMonth);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetAggregatedByMonthAsync_WhenMeteringPointExists_ReturnsValidAggregatedMeasurements()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        var expectedDate = Instant.FromUtc(2021, 2, 1, 23, 0, 0).ToDateOnly();
        var expectedYearMonth = new YearMonth(expectedDate.Year, expectedDate.Month);
        var url = CreateGetAggregatedMeasurementsByMonthUrl(expectedMeteringPointId, new Year(expectedYearMonth.Year));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<MeasurementsAggregatedByMonthResponse>(actualResponse);

        // Assert
        Assert.Equal(HttpStatusCode.OK, actualResponse.StatusCode);
        Assert.Single(actual.MeasurementAggregations);
        Assert.Equal(expectedYearMonth, actual.MeasurementAggregations.First().YearMonth);
        foreach (var measurementAggregation in actual.MeasurementAggregations)
        {
            Assert.Equal(Quality.Measured, measurementAggregation.Quality);
            Assert.Equal(571.2M, measurementAggregation.Quantity);
            Assert.Equal(Unit.kWh, measurementAggregation.Unit);
        }
    }

    [Fact]
    public async Task GetAggregatedByMonthAsync_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
    {
        // Arrange
        const string expectedMeteringPointId = "not existing id";
        var yearMonth = new YearMonth(2022, 1);
        var url = CreateGetAggregatedMeasurementsByMonthUrl(expectedMeteringPointId, new Year(yearMonth.Year));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetAggregatedByYearAsync_WhenMeteringPointExists_ReturnsValidAggregatedMeasurements()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        var url = CreateGetAggregatedMeasurementsByYearUrl(expectedMeteringPointId);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<MeasurementsAggregatedByYearResponse>(actualResponse);

        // Assert
        Assert.Equal(HttpStatusCode.OK, actualResponse.StatusCode);
        Assert.Equal(2, actual.MeasurementAggregations.Count);

        var firstMeasurementAggregation = actual.MeasurementAggregations.First();
        Assert.Equal(2021, firstMeasurementAggregation.Year);
        Assert.Equal(Quality.Measured, firstMeasurementAggregation.Quality);
        Assert.Equal(571.2M, firstMeasurementAggregation.Quantity);
        Assert.Equal(Unit.kWh, firstMeasurementAggregation.Unit);

        var lastMeasurementAggregation = actual.MeasurementAggregations.Last();
        Assert.Equal(2022, lastMeasurementAggregation.Year);
        Assert.Equal(Quality.Measured, lastMeasurementAggregation.Quality);
        Assert.Equal(1428M, lastMeasurementAggregation.Quantity);
        Assert.Equal(Unit.kWh, lastMeasurementAggregation.Unit);
    }

    [Fact]
    public async Task GetAggregatedByYearAsync_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
    {
        // Arrange
        const string expectedMeteringPointId = "not existing id";
        var url = CreateGetAggregatedMeasurementsByYearUrl(expectedMeteringPointId);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    [Theory]
    [InlineData("")]
    [InlineData("v1")]
    [InlineData("v4")]
    public async Task GetAggregatedMeasurementsAsync_WhenTargetingUnsupportedVersion_ReturnsNotFound(string version)
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        var yearMonth = new YearMonth(2021, 2);
        var url = CreateGetAggregatedMeasurementsByMonthV2Url(expectedMeteringPointId, yearMonth, version);

        // Act
        var actual = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actual.StatusCode);
    }

    private static string CreateGetMeasurementsForPeriodUrl(string expectedMeteringPointId, string startDate, string endDate, string versionPrefix = "v3")
    {
        return $"{versionPrefix}/measurements/forPeriod?meteringPointId={expectedMeteringPointId}&startDate={startDate}&endDate={endDate}";
    }

    private static string CreateGetAggregatedMeasurementsByMonthV2Url(string expectedMeteringPointId, YearMonth yearMonth, string versionPrefix = "v2")
    {
        return $"{versionPrefix}/measurements/aggregatedByMonth?meteringPointId={expectedMeteringPointId}&year={yearMonth.Year}&month={yearMonth.Month}";
    }

    private static string CreateGetAggregatedMeasurementsByYearV2Url(string expectedMeteringPointId, Year year, string versionPrefix = "v2")
    {
        return $"{versionPrefix}/measurements/aggregatedByYear?meteringPointId={expectedMeteringPointId}&year={year.GetYear()}";
    }

    private static string CreateGetAggregatedMeasurementsByDateUrl(string expectedMeteringPointId, YearMonth yearMonth, string versionPrefix = "v3")
    {
        return $"{versionPrefix}/measurements/aggregatedByDate?meteringPointId={expectedMeteringPointId}&year={yearMonth.Year}&month={yearMonth.Month}";
    }

    private static string CreateGetAggregatedMeasurementsByMonthUrl(string expectedMeteringPointId, Year year, string versionPrefix = "v3")
    {
        return $"{versionPrefix}/measurements/aggregatedByMonth?meteringPointId={expectedMeteringPointId}&year={year.GetYear()}";
    }

    private static string CreateGetAggregatedMeasurementsByYearUrl(string expectedMeteringPointId, string versionPrefix = "v3")
    {
        return $"{versionPrefix}/measurements/aggregatedByYear?meteringPointId={expectedMeteringPointId}";
    }

    private async Task<T> ParseResponseAsync<T>(HttpResponseMessage response)
        where T : class
    {
        response.EnsureSuccessStatusCode();
        var actualBody = await response.Content.ReadAsStringAsync();
        return new JsonSerializer().Deserialize<T>(actualBody);
    }
}

using System.Globalization;
using System.Net;
using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Extensions;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Controllers;

[IntegrationTest]
public class MeasurementsControllerTests(WebApiFixture fixture) : IClassFixture<WebApiFixture>, IAsyncLifetime
{
    public async Task InitializeAsync()
    {
        await fixture.CreateTableAsync();
    }

    public async Task DisposeAsync()
    {
        await fixture.DeleteTableAsync();
    }

    [Fact]
    public async Task GetByPeriodAsync_WhenMeteringPointExists_ReturnsValidMeasurements()
    {
        // Arrange
        const string meteringPointId = "123456789012345678";
        var rows = new MeasurementsTableRowsBuilder()
            .WithContinuousRowsForDate(meteringPointId, new LocalDate(2022, 3, 20))
            .Build();
        await fixture.InsertRowsAsync(rows);
        var url = CreateGetMeasurementsForPeriodUrl(
            meteringPointId, "2022-03-19T23:00:00Z", "2022-03-20T23:00:00Z");

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
            Assert.Equal("2022-03-24T23:00:00Z", point.Created.ToString("yyyy-MM-ddTHH:mm:ss'Z'", CultureInfo.InvariantCulture));
        }
    }

    [Fact]
    public async Task GetByPeriodAsync_WhenMultipleObservations_ReturnsAllObservations()
    {
        // Arrange
        const string meteringPointId = "123456789012345678";
        var startDate = new LocalDate(2022, 2, 15);
        var endDate = new LocalDate(2022, 2, 16);
        var rows = new MeasurementsTableRowsBuilder()
            .WithContinuousRowsForDate(meteringPointId, startDate)
            .WithContinuousRowsForDate(meteringPointId, startDate)
            .Build();
        await fixture.InsertRowsAsync(rows);
        var url = CreateGetMeasurementsForPeriodUrl(
            meteringPointId, startDate.ToUtcString(), endDate.ToUtcString());

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<MeasurementsResponse>(actualResponse);

        // Assert
        Assert.Equal(2, actual.Points.Count(p => p.ObservationTime.ToString().Equals(startDate.ToUtcString())));
    }

    [Fact]
    public async Task GetByPeriodAsync_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
    {
        // Arrange
        var url = CreateGetMeasurementsForPeriodUrl(
            "987654321987654321", "2022-01-31T23:00:00Z", "2022-01-01T23:00:00Z");

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetByPeriodAsync_WhenInvalidMeteringPointId_ReturnBadRequest()
    {
        // Arrange
        var url = CreateGetMeasurementsForPeriodUrl(
            "invalid metering point id", "2022-01-31T23:00:00Z", "2022-01-01T23:00:00Z");

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.BadRequest, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetByPeriodAsync_WhenMeasurementHasInvalidUnit_ReturnInternalServerError()
    {
        // Arrange
        const string meteringPointId = "123456789012345678";
        const string startDate = "2022-01-31T23:00:00Z";
        const string endDate = "2022-02-01T23:00:00Z";
        var invalidRow = new MeasurementTableRowBuilder()
            .WithMeteringPointId(meteringPointId)
            .WithObservationTime("2022-01-31T23:00:00Z")
            .WithUnit("invalid unit")
            .Build();
        var rows = new MeasurementsTableRowsBuilder().WithRow(invalidRow).Build();
        await fixture.InsertRowsAsync(rows);
        var url = CreateGetMeasurementsForPeriodUrl(meteringPointId, startDate, endDate);

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
        const string meteringPointId = "123456789012345678";
        const string startDate = "2022-01-31T23:00:00Z";
        const string endDate = "2022-02-01T23:00:00Z";
        var invalidRow = new MeasurementTableRowBuilder()
            .WithMeteringPointId(meteringPointId)
            .WithObservationTime("2022-01-31T23:00:00Z")
            .WithResolution("invalid resolution")
            .Build();
        var rows = new MeasurementsTableRowsBuilder().WithRow(invalidRow).Build();
        await fixture.InsertRowsAsync(rows);
        var url = CreateGetMeasurementsForPeriodUrl(meteringPointId, startDate, endDate);

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
        const string meteringPointId = "123456789123456789";
        var rows = new MeasurementsTableRowsBuilder()
            .WithContinuousRowsForDate(meteringPointId, new LocalDate(2021, 2, 1))
            .WithContinuousRowsForDate(meteringPointId, new LocalDate(2021, 2, 2))
            .WithContinuousRowsForDate(meteringPointId, new LocalDate(2021, 2, 3))
            .Build();
        await fixture.InsertRowsAsync(rows);
        var url = CreateGetAggregatedMeasurementsByMonthV2Url(meteringPointId, new YearMonth(2021, 2));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<MeasurementsAggregatedByDateResponse>(actualResponse);

        // Assert
        Assert.Equal(HttpStatusCode.OK, actualResponse.StatusCode);
        Assert.Equal(3, actual.MeasurementAggregations.Count);
        Assert.Equal(new DateOnly(2021, 2, 1), actual.MeasurementAggregations.First().Date);
        foreach (var measurementAggregation in actual.MeasurementAggregations)
        {
            Assert.Equal(Quality.Measured, measurementAggregation.Quality);
            Assert.Equal(24m, measurementAggregation.Quantity);
            Assert.False(measurementAggregation.MissingValues);
            Assert.Equal(Unit.kWh, measurementAggregation.Unit);
            Assert.False(measurementAggregation.ContainsUpdatedValues);
        }
    }

    [Fact]
    public async Task GetAggregatedByMonthAsyncV2_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
    {
        // Arrange
        var url = CreateGetAggregatedMeasurementsByMonthV2Url(
            "987654321987654321", new YearMonth(2022, 1));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetAggregatedByMonthAsyncV2_WhenInvalidMeteringPointId_ReturnBadRequest()
    {
        // Arrange
        var url = CreateGetAggregatedMeasurementsByMonthV2Url(
            "invalid metering point id ", new YearMonth(2022, 1));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.BadRequest, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetAggregatedByYearAsyncV2_WhenMeteringPointExists_ReturnsValidAggregatedMeasurements()
    {
        // Arrange
        const string meteringPointId = "123456789123456789";
        const int year = 2021;
        var rows = new MeasurementsTableRowsBuilder()
            .WithContinuousRowsForDate(meteringPointId, new LocalDate(year, 2, 5))
            .WithContinuousRowsForDate(meteringPointId, new LocalDate(year, 3, 6))
            .WithContinuousRowsForDate(meteringPointId, new LocalDate(year, 3, 7))
            .Build();
        await fixture.InsertRowsAsync(rows);
        var url = CreateGetAggregatedMeasurementsByYearV2Url(meteringPointId, new Year(year));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<MeasurementsAggregatedByMonthResponse>(actualResponse);

        // Assert
        Assert.Equal(HttpStatusCode.OK, actualResponse.StatusCode);

        var actualFirst = actual.MeasurementAggregations.First();
        Assert.Equal(year, actualFirst.YearMonth.Year);
        Assert.Equal(2, actualFirst.YearMonth.Month);
        Assert.Equal(Quality.Measured, actualFirst.Quality);
        Assert.Equal(24.0m, actualFirst.Quantity);
        Assert.Equal(Unit.kWh, actualFirst.Unit);

        var actualLast = actual.MeasurementAggregations.Last();
        Assert.Equal(year, actualLast.YearMonth.Year);
        Assert.Equal(3, actualLast.YearMonth.Month);
        Assert.Equal(Quality.Measured, actualLast.Quality);
        Assert.Equal(48.0m, actualLast.Quantity);
        Assert.Equal(Unit.kWh, actualLast.Unit);
    }

    [Fact]
    public async Task GetAggregatedByYearAsyncV2_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
    {
        // Arrange
        var url = CreateGetAggregatedMeasurementsByYearV2Url(
            "987654321987654321", new Year(2021));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetAggregatedByYearAsyncV2_WhenInvalidMeteringPoint_ReturnBadRequest()
    {
        // Arrange
        var url = CreateGetAggregatedMeasurementsByYearV2Url(
            "invalid metering point id", new Year(2022));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.BadRequest, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetAggregatedByDateAsync_WhenMeteringPointExists_ReturnsValidAggregatedMeasurements()
    {
        // Arrange
        const string meteringPointId = "123456789098765432";
        var rows = new MeasurementsTableRowsBuilder()
            .WithContinuousRowsForDate($"{meteringPointId}", new LocalDate(2023, 2, 2))
            .WithContinuousRowsForDate($"{meteringPointId}", new LocalDate(2023, 2, 3))
            .Build();
        await fixture.InsertRowsAsync(rows);

        var yearMonth = new YearMonth(2023, 2);
        var url = CreateGetAggregatedMeasurementsByDateUrl(meteringPointId, yearMonth);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<MeasurementsAggregatedByDateResponse>(actualResponse);

        // Assert
        Assert.Equal(HttpStatusCode.OK, actualResponse.StatusCode);
        Assert.Equal(2, actual.MeasurementAggregations.Count);
        Assert.Equal(new DateOnly(2023, 2, 2), actual.MeasurementAggregations.First().Date);
        Assert.Equal(new DateOnly(2023, 2, 3), actual.MeasurementAggregations.Last().Date);
        foreach (var measurementAggregation in actual.MeasurementAggregations)
        {
            Assert.Equal(Quality.Measured, measurementAggregation.Quality);
            Assert.Equal(24.0M, measurementAggregation.Quantity);
            Assert.Equal(Unit.kWh, measurementAggregation.Unit);
            Assert.False(measurementAggregation.MissingValues);
            Assert.False(measurementAggregation.ContainsUpdatedValues);
        }
    }

    [Fact]
    public async Task GetAggregatedByDateAsync_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
    {
        // Arrange
        var url = CreateGetAggregatedMeasurementsByDateUrl(
            "987654321987654321", new YearMonth(2022, 1));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetAggregatedByDateAsync_WhenInvalidMeteringPointId_ReturnBadRequest()
    {
        // Arrange
        var url = CreateGetAggregatedMeasurementsByDateUrl(
            "invalid metering point id", new YearMonth(2022, 1));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.BadRequest, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetAggregatedByMonthAsync_WhenMeteringPointExists_ReturnsValidAggregatedMeasurements()
    {
        // Arrange
        const string meteringPointId = "123456789123456789";
        const int year = 2021;
        var rows = new MeasurementsTableRowsBuilder()
            .WithContinuousRowsForDate(meteringPointId, new LocalDate(year, 2, 5))
            .WithContinuousRowsForDate(meteringPointId, new LocalDate(year, 3, 6))
            .WithContinuousRowsForDate(meteringPointId, new LocalDate(year, 4, 7))
            .Build();
        await fixture.InsertRowsAsync(rows);
        var url = CreateGetAggregatedMeasurementsByMonthUrl(meteringPointId, new Year(year));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<MeasurementsAggregatedByMonthResponse>(actualResponse);

        // Assert
        Assert.Equal(HttpStatusCode.OK, actualResponse.StatusCode);
        Assert.All(actual.MeasurementAggregations, x => Assert.Equal(year, x.YearMonth.Year));
        Assert.Equal(3, actual.MeasurementAggregations.Count);
        foreach (var measurementAggregation in actual.MeasurementAggregations)
        {
            Assert.Equal(Quality.Measured, measurementAggregation.Quality);
            Assert.Equal(24.0m, measurementAggregation.Quantity);
            Assert.Equal(Unit.kWh, measurementAggregation.Unit);
        }
    }

    [Fact]
    public async Task GetAggregatedByDateAsync_WhenDateContainsMissingValues_TheFlagIsSetInResponse()
    {
        // Arrange
        const string meteringPointId = "123456789098765432";
        var date = new LocalDate(2023, 4, 5);
        var rows = new MeasurementsTableRowsBuilder()
            .WithContinuousRows(meteringPointId, date, 10)
            .Build();
        await fixture.InsertRowsAsync(rows);
        var url = CreateGetAggregatedMeasurementsByDateUrl(meteringPointId, new YearMonth(date.Year, date.Month));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<MeasurementsAggregatedByDateResponse>(actualResponse);

        // Assert
        Assert.True(actual.MeasurementAggregations.First().MissingValues);
    }

    [Fact]
    public async Task GetAggregatedByMonthAsync_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
    {
        // Arrange
        var url = CreateGetAggregatedMeasurementsByMonthUrl(
            "987654321987654321", new Year(2022));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetAggregatedByYearAsync_WhenMeteringPointExists_ReturnsValidAggregatedMeasurements()
    {
        // Arrange
        const string meteringPointId = "123456789123456789";
        var rows = new MeasurementsTableRowsBuilder()
            .WithContinuousRowsForDate(meteringPointId, new LocalDate(2021, 2, 5))
            .WithContinuousRowsForDate(meteringPointId, new LocalDate(2022, 3, 6))
            .WithContinuousRowsForDate(meteringPointId, new LocalDate(2022, 4, 7))
            .Build();
        await fixture.InsertRowsAsync(rows);
        var url = CreateGetAggregatedMeasurementsByYearUrl(meteringPointId);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<MeasurementsAggregatedByYearResponse>(actualResponse);

        // Assert
        Assert.Equal(HttpStatusCode.OK, actualResponse.StatusCode);
        Assert.Equal(2, actual.MeasurementAggregations.Count);

        var firstMeasurementAggregation = actual.MeasurementAggregations.First();
        Assert.Equal(2021, firstMeasurementAggregation.Year);
        Assert.Equal(Quality.Measured, firstMeasurementAggregation.Quality);
        Assert.Equal(24.0m, firstMeasurementAggregation.Quantity);
        Assert.Equal(Unit.kWh, firstMeasurementAggregation.Unit);

        var lastMeasurementAggregation = actual.MeasurementAggregations.Last();
        Assert.Equal(2022, lastMeasurementAggregation.Year);
        Assert.Equal(Quality.Measured, lastMeasurementAggregation.Quality);
        Assert.Equal(48m, lastMeasurementAggregation.Quantity);
        Assert.Equal(Unit.kWh, lastMeasurementAggregation.Unit);
    }

    [Fact]
    public async Task GetAggregatedByYearAsync_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
    {
        // Arrange
        const string expectedMeteringPointId = "987654321987654321";
        var url = CreateGetAggregatedMeasurementsByYearUrl(expectedMeteringPointId);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetAggregatedByMonthAsync_WhenInvalidMeteringPointId_ReturnBadRequest()
    {
        // Arrange
        var url = CreateGetAggregatedMeasurementsByMonthUrl(
            "invalid metering point id", new Year(2022));

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.BadRequest, actualResponse.StatusCode);
    }

    [Theory]
    [InlineData("", HttpStatusCode.NotFound)]
    [InlineData("v1", HttpStatusCode.NotFound)]
    [InlineData("v2", HttpStatusCode.OK)]
    [InlineData("v3", HttpStatusCode.OK)]
    [InlineData("v4", HttpStatusCode.NotFound)]
    public async Task GetAggregatedByMonthAsyncV2_WhenTargetingUnsupportedVersions_ReturnsNotFound(
        string version, HttpStatusCode expectedStatusCode)
    {
        // Arrange
        const string meteringPointId = "123456789123456789";
        var yearMonth = new YearMonth(2021, 2);
        var rows = new MeasurementsTableRowsBuilder()
            .WithContinuousRowsForDate(meteringPointId, new LocalDate(yearMonth.Year, yearMonth.Month, 5))
            .Build();
        await fixture.InsertRowsAsync(rows);
        var url = CreateGetAggregatedMeasurementsByMonthV2Url(meteringPointId, yearMonth, version);

        // Act
        var actual = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(expectedStatusCode, actual.StatusCode);
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

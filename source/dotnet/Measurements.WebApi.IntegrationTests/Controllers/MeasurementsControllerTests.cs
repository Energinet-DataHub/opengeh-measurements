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
    public async Task GetMeasurementsAsync_WhenMeteringPointExists_ReturnsValidMeasurements()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        const string startDate = "2022-01-01T23:00:00Z";
        const string endDate = "2022-01-02T23:00:00Z";
        var url = CreateGetMeasurementsForPeriodUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<GetMeasurementResponse>(actualResponse);

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
    public async Task GetMeasurementsAsync_WhenMultipleObservations_ReturnsAllObservations()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        const string startDate = "2022-01-02T23:00:00Z"; // On this date, the fixture inserts 2 measurements with the same observation time
        const string endDate = "2022-01-03T23:00:00Z";
        var url = CreateGetMeasurementsForPeriodUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<GetMeasurementResponse>(actualResponse);

        // Assert
        Assert.Equal(2, actual.Points.Count(p => p.ObservationTime.ToString().Equals(startDate)));
    }

    [Fact]
    public async Task GetMeasurementsAsync_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
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
    public async Task GetMeasurementsAsync_WhenMeasurementHasInvalidQuality_ReturnInternalServerError()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        const string startDate = "2022-01-31T23:00:00Z"; // On this date, the fixture inserts a measurement with invalid quality
        const string endDate = "2022-02-01T23:00:00Z";
        var url = CreateGetMeasurementsForPeriodUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actual = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.InternalServerError, actual.StatusCode);
    }

    [Fact]
    public async Task GetAggregatedMeasurementsAsync_WhenMeteringPointExists_ReturnsValidAggregatedMeasurements()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        var expectedDate = Instant.FromUtc(2021, 2, 1, 23, 0, 0).ToDateOnly();
        var yearMonth = new YearMonth(2021, 2);
        var url = CreateGetAggregatedMeasurementsByMonthUrl(expectedMeteringPointId, yearMonth);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);
        var actual = await ParseResponseAsync<GetAggregatedMeasurementsResponse>(actualResponse);

        // Assert
        Assert.Equal(HttpStatusCode.OK, actualResponse.StatusCode);
        Assert.Equal(2, actual.MeasurementAggregations.Count);
        Assert.Equal(expectedDate, actual.MeasurementAggregations.First().Date);
        foreach (var measurementAggregation in actual.MeasurementAggregations)
        {
            Assert.Equal(Quality.Measured, measurementAggregation.Quality);
            Assert.Equal(285.6M, measurementAggregation.Quantity);
            Assert.False(measurementAggregation.MissingValues);
        }
    }

    [Fact]
    public async Task GetAggregatedMeasurementsAsync_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
    {
        // Arrange
        const string expectedMeteringPointId = "not existing id";
        var yearMonth = new YearMonth(2022, 1);
        var url = CreateGetAggregatedMeasurementsByMonthUrl(expectedMeteringPointId, yearMonth);

        // Act
        var actualResponse = await fixture.Client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    private static string CreateGetMeasurementsForPeriodUrl(string expectedMeteringPointId, string startDate, string endDate)
    {
        return $"measurements/forPeriod?meteringPointId={expectedMeteringPointId}&startDate={startDate}&endDate={endDate}";
    }

    private static string CreateGetAggregatedMeasurementsByMonthUrl(string expectedMeteringPointId, YearMonth yearMonth)
    {
        return $"measurements/aggregatedByMonth?meteringPointId={expectedMeteringPointId}&year={yearMonth.Year}&month={yearMonth.Month}";
    }

    private async Task<T> ParseResponseAsync<T>(HttpResponseMessage response)
        where T : class
    {
        response.EnsureSuccessStatusCode();
        var actualBody = await response.Content.ReadAsStringAsync();
        return new JsonSerializer().Deserialize<T>(actualBody);
    }
}

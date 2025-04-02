using System.Net;
using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;
using NodaTime;
using NodaTime.Text;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Controllers;

[IntegrationTest]
public class MeasurementsControllerTests(WebApiFixture fixture)
    : IClassFixture<WebApiFixture>
{
    private readonly HttpClient _client = fixture.CreateClient();

    [Fact]
    public async Task GetAsync_WhenMeteringPointExists_ReturnsValidMeasurements()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        const string startDate = "2022-01-03T00:00:00Z";
        const string endDate = "2022-01-04T00:00:00Z";
        var url = CreateUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actualResponse = await _client.GetAsync(url);
        var actual = await ParseResponseAsync<GetMeasurementResponse>(actualResponse);

        // Assert
        Assert.Equal(24, actual.Points.Count);
        Assert.True(actual.Points.All(p => p.Unit == Unit.kWh));
        Assert.True(actual.Points.All(p => p.Quality == Quality.Measured));
    }

    [Fact]
    public async Task GetAsync_WhenMultipleMeasurementsExistWithEqualObservationTime_ReturnsOnlyOnePerObservationTime()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        const string startDate = "2022-01-03T00:00:00Z"; // On this date, the fixture inserts multiple measurements with the same observation time
        const string endDate = "2022-01-04T00:00:00Z";
        var url = CreateUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actualResponse = await _client.GetAsync(url);
        var actual = await ParseResponseAsync<GetMeasurementResponse>(actualResponse);

        // Assert
        Assert.Equal(24, actual.Points.Count);
    }

    [Fact]
    public async Task GetAsync_WhenCancelledMeasurementsExists_ReturnsNotFoundStatus()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        const string startDate = "2022-01-02T00:00:00Z";  // On this date, the fixture inserts both cancelled and non-cancelled measurements with the same observation time
        const string endDate = "2022-01-03T00:00:00Z";
        var url = CreateUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actualResponse = await _client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetAsync_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
    {
        // Arrange
        const string expectedMeteringPointId = "not existing id";
        const string startDate = "2021-01-02T00:00:00Z"; // On this date, the fixture inserts a measurement with invalid quality
        const string endDate = "2021-01-03T00:00:00Z";
        var url = CreateUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actualResponse = await _client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    [Fact]
    public async Task GetAsync_WhenMeasurementHasInvalidQuality_ReturnInternalServerError()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        const string startDate = "2022-02-01T00:00:00Z";
        const string endDate = "2022-02-02T00:00:00Z";
        var url = CreateUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actual = await _client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.InternalServerError, actual.StatusCode);
    }

    [Fact]
    public async Task GetAggregatedAsync_WhenMeteringPointExists_ReturnsValidAggregatedMeasurements()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        var expectedFirstMinObservationTime = InstantPattern.ExtendedIso.Parse("2022-01-02T23:00:00Z").Value;
        var expectedFirstMaxObservationTime = InstantPattern.ExtendedIso.Parse("2022-01-03T22:00:00Z").Value;
        var yearMonth = new YearMonth(2022, 1);
        var url = CreateUrl(expectedMeteringPointId, yearMonth);

        // Act
        var actualResponse = await _client.GetAsync(url);
        var actual = await ParseResponseAsync<GetAggregatedMeasurementsResponse>(actualResponse);

        // Assert
        Assert.Equal(HttpStatusCode.OK, actualResponse.StatusCode);
        Assert.Equal(2, actual.MeasurementAggregations.Count);
        Assert.Equal(expectedFirstMinObservationTime, actual.MeasurementAggregations.First().MinObservationTime);
        Assert.Equal(expectedFirstMaxObservationTime, actual.MeasurementAggregations.First().MaxObservationTime);
        Assert.True(actual.MeasurementAggregations.All(p => p.Qualities.All(q => q == Quality.Measured)));
        Assert.True(actual.MeasurementAggregations.All(p => p.PointCount == 24));
        Assert.True(actual.MeasurementAggregations.All(p => p.Quantity == 285.6M));
    }

    private static string CreateUrl(string expectedMeteringPointId, string startDate, string endDate)
    {
        return $"measurements/forPeriod?meteringPointId={expectedMeteringPointId}&startDate={startDate}&endDate={endDate}";
    }

    private static string CreateUrl(string expectedMeteringPointId, YearMonth yearMonth)
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

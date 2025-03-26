using System.Net;
using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;
using NodaTime;
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
        const string startDate = "2022-01-02T00:00:00Z";
        const string endDate = "2022-01-03T00:00:00Z";
        var url = CreateUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actualResponse = await _client.GetAsync(url);
        var actual = await ParseResponseAsync(actualResponse);

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
        var actual = await ParseResponseAsync(actualResponse);

        // Assert
        Assert.Equal(24, actual.Points.Count);
    }

    [Fact]
    public async Task GetAsync_WhenCancelledMeasurementsExists_ReturnsOnlyNonCancelledObservations()
    {
        // Arrange
        const string expectedMeteringPointId = "1234567890";
        const string startDate = "2022-01-02T00:00:00Z";  // On this date, the fixture inserts both cancelled and non-cancelled measurements with the same observation time
        const string endDate = "2022-01-03T00:00:00Z";
        var url = CreateUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actualResponse = await _client.GetAsync(url);
        var actual = await ParseResponseAsync(actualResponse);

        // Assert
        Assert.Equal(24, actual.Points.Count);
        Assert.True(actual.Points.All(p => p.Unit == Unit.kWh));
        Assert.True(actual.Points.All(p => p.Quality == Quality.Measured));
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
        var yearMonth = new YearMonth(2022, 1);
        var url = CreateUrl(expectedMeteringPointId, yearMonth);

        // Act
        var actualResponse = await _client.GetAsync(url);
        // var actual = await ParseResponseAsync(actualResponse);

        // Assert
        Assert.True(actualResponse.StatusCode == HttpStatusCode.OK);
        // Assert.Equal(31, actual.Points.Count);
        // Assert.True(actual.Points.All(p => p.Unit == Unit.kWh));
        // Assert.True(actual.Points.All(p => p.Quality == Quality.Measured));*/
    }

    private static string CreateUrl(string expectedMeteringPointId, string startDate, string endDate)
    {
        return $"measurements?meteringPointId={expectedMeteringPointId}&startDate={startDate}&endDate={endDate}";
    }

    private static string CreateUrl(string expectedMeteringPointId, YearMonth yearMonth)
    {
        return $"measurements?meteringPointId={expectedMeteringPointId}&year={yearMonth.Year}&month={yearMonth.Month}";
    }

    private async Task<GetMeasurementResponse> ParseResponseAsync(HttpResponseMessage response)
    {
        response.EnsureSuccessStatusCode();
        var actualBody = await response.Content.ReadAsStringAsync();
        return new JsonSerializer().Deserialize<GetMeasurementResponse>(actualBody);
    }
}

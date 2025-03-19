using System.Net;
using Energinet.DataHub.Measurements.Application.Responses;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Serialization;
using Energinet.DataHub.Measurements.WebApi.IntegrationTests.Fixtures;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.WebApi.IntegrationTests.Controllers;

[IntegrationTest]
public class MeasurementsControllerTests(WebApiFixture fixture)
    : IClassFixture<WebApiFixture>
{
    private readonly HttpClient _client = fixture.CreateClient();

    [Fact]
    public async Task GetAsync_WhenMeteringPointExists_ReturnsValidMeasurement()
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
        const string startDate = "2022-01-05T00:00:00Z";
        const string endDate = "2022-01-06T00:00:00Z";
        var url = CreateUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actualResponse = await _client.GetAsync(url);
        var actual = await ParseResponseAsync(actualResponse);

        // Assert
        Assert.Equal(24, actual.Points.Count);
    }

    [Fact]
    public async Task GetAsync_WhenMeteringPointDoesNotExist_ReturnNotFoundStatus()
    {
        // Arrange
        const string expectedMeteringPointId = "not existing id";
        const string startDate = "2021-01-02T00:00:00Z";
        const string endDate = "2021-01-03T00:00:00Z";
        var url = CreateUrl(expectedMeteringPointId, startDate, endDate);

        // Act
        var actualResponse = await _client.GetAsync(url);

        // Assert
        Assert.Equal(HttpStatusCode.NotFound, actualResponse.StatusCode);
    }

    private static string CreateUrl(string expectedMeteringPointId, string startDate, string endDate)
    {
        return $"measurements?meteringPointId={expectedMeteringPointId}&startDate={startDate}&endDate={endDate}";
    }

    private async Task<GetMeasurementResponse> ParseResponseAsync(HttpResponseMessage response)
    {
        response.EnsureSuccessStatusCode();
        var actualBody = await response.Content.ReadAsStringAsync();
        return new JsonSerializer().Deserialize<GetMeasurementResponse>(actualBody);
    }
}

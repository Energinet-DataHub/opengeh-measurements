using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Tests.Fixtures;
using NodaTime;

namespace Energinet.DataHub.Measurements.Client.Tests;

[Collection(nameof(MeasurementsClientCollection))]
public class MeasurementsClientTests
{
    private MeasurementsClientFixture Fixture { get; }

    private MeasurementsClient MeasurementsClient { get; }

    public MeasurementsClientTests(MeasurementsClientFixture fixture)
    {
        Fixture = fixture;
        MeasurementsClient = new MeasurementsClient(new FakeHttpClientFactory(Fixture.HttpClient));
    }

    [Theory]
    [InlineData(2025, 1, 2)]
    [InlineData(2025, 6, 15)]
    public async Task GetMeasurementsForDayAsync_WhenCalledWithValidQuery_ReturnsMeasurementDto(
        int year, int month, int day)
    {
        // Arrange
        var query = new GetMeasurementsForDayQuery(
            "1234567890",
            new LocalDate(year, month, day));

        // Act
        var result = await MeasurementsClient.GetMeasurementsForDayAsync(query, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(24, result.Points.Count);
        Assert.True(result.Points.All(p => p.Quality == Quality.Measured));
    }

    [Fact]
    public async Task GetMeasurementsForDayAsync_WhenCalledWithQueryWithNoMeasurements_ReturnsEmptyListOfPoints()
    {
        // Arrange
        var query = new GetMeasurementsForDayQuery(
            "1234567890",
            new LocalDate(1990, 1, 2));

        // Act
        var actual = await MeasurementsClient.GetMeasurementsForDayAsync(query, CancellationToken.None);

        // Assert
        Assert.Empty(actual.Points);
    }
}

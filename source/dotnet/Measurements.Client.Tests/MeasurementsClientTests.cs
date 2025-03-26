using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Tests.Fixtures;
using NodaTime;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.Client.Tests;

[IntegrationTest]
[Collection(nameof(MeasurementsClientCollection))]
public class MeasurementsClientTests
{
    private MeasurementsClient MeasurementsClient { get; }

    public MeasurementsClientTests(MeasurementsClientFixture fixture)
    {
        MeasurementsClient = new MeasurementsClient(new FakeHttpClientFactory(fixture.HttpClient));
    }

    [Theory]
    [InlineData(2023, 1, 2)]
    [InlineData(2023, 6, 15)]
    public async Task GetMeasurementsForDayAsync_WhenCalledWithValidQuery_ReturnsListOfPoints(
        int year, int month, int day)
    {
        // Arrange
        var query = new GetMeasurementsForDayQuery(
            "1234567890",
            new LocalDate(year, month, day));

        // Act
        var actual = (await MeasurementsClient.GetMeasurementsForDayAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(24, actual.Count);
        Assert.True(actual.All(p => p.Quality == Quality.Measured));
    }

    [Fact]
    public async Task GetMeasurementsForDayAsync_WhenCalledWithQueryWithNoMeasurements_ReturnsEmptyList()
    {
        // Arrange
        var query = new GetMeasurementsForDayQuery(
            "1234567890",
            new LocalDate(1990, 1, 2));

        // Act
        var actual = await MeasurementsClient.GetMeasurementsForDayAsync(query, CancellationToken.None);

        // Assert
        Assert.Empty(actual);
    }

    [Fact]
    public async Task GetMeasurementsForPeriodAsync_WhenCalledWithValidQuery_ReturnsListOfPoints()
    {
        // Arrange
        var from = new LocalDate(2023, 1, 2);
        var to = new LocalDate(2023, 1, 6);
        var query = new GetMeasurementsForPeriodQuery("1234567890", from, to);

        // Act
        var actual = (await MeasurementsClient.GetMeasurementsForPeriodAsync(query, CancellationToken.None)).ToList();

        // Assert
        Assert.NotNull(actual);
        Assert.Equal(96, actual.Count);
        Assert.True(actual.All(p => p.Quality == Quality.Measured));
    }
}

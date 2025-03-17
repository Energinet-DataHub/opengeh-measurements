using System.Net;
using Energinet.DataHub.Measurements.Abstractions.Api.Models;
using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Application.Exceptions;
using Energinet.DataHub.Measurements.Client.Tests.Fixtures;

namespace Energinet.DataHub.Measurements.Client.Tests;

[Collection(nameof(MeasurementsClientCollection))]
public class MeasurementsClientTests
{
    private MeasurementsClientFixture Fixture { get; }

    private IMeasurementsClient MeasurementsClient { get; }

    public MeasurementsClientTests(MeasurementsClientFixture fixture)
    {
        Fixture = fixture;
        MeasurementsClient = new MeasurementsClient(new FakeHttpClientFactory(Fixture.HttpClient));
    }

    [Fact]
    public async Task GetMeasurementsForDayAsync_WhenCalledWithValidQuery_ReturnsMeasurementDto()
    {
        // Arrange
        var query = new GetMeasurementsForDayQuery(
            "1234567890",
            new DateTimeOffset(2025, 1, 2, 23, 0, 0, TimeSpan.Zero));

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
        const string meteringPointId = "1234567890";
        var query = new GetMeasurementsForDayQuery(
            meteringPointId,
            new DateTimeOffset(1900, 1, 2, 0, 0, 0, TimeSpan.Zero));

        // Act
        var actual = await MeasurementsClient.GetMeasurementsForDayAsync(query, CancellationToken.None);

        // Assert
        Assert.Empty(actual.Points);
    }
}

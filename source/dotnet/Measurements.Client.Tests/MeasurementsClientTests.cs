using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.Client.Extensions.Options;
using Energinet.DataHub.Measurements.Client.Tests.Extensions;
using Energinet.DataHub.Measurements.Client.Tests.Fixtures;
using Microsoft.Extensions.DependencyInjection;

namespace Energinet.DataHub.Measurements.Client.Tests;

[Collection(nameof(MeasurementsClientAppCollection))]
public class MeasurementsClientTests
{
    private MeasurementsClientAppFixture Fixture { get; }

    private ServiceProvider ServiceProvider { get; }

    private IMeasurementsClient MeasurementsClient { get; }

    public MeasurementsClientTests(MeasurementsClientAppFixture fixture)
    {
        Fixture = fixture;

        var services = new ServiceCollection();
        services.AddInMemoryConfiguration(new Dictionary<string, string?>
        {
            [$"{MeasurementHttpClientOptions.SectionName}:{nameof(MeasurementHttpClientOptions.BaseAddress)}"]
                = Fixture.HttpClient.BaseAddress!.ToString(),
        });
        services.AddMeasurementsClient();
        ServiceProvider = services.BuildServiceProvider();
        MeasurementsClient = ServiceProvider.GetRequiredService<IMeasurementsClient>();
    }

    [Fact]
    public async Task GetMeasurementsForDayAsync_WhenCalledWithValidQuery_ReturnsMeasurementDto()
    {
        // Arrange
        var query = new GetMeasurementsForDayQuery(
            "MeteringPointId",
            new DateTimeOffset(2025, 3, 1, 0, 0, 0, TimeSpan.Zero));

        // Act
        var result = await MeasurementsClient.GetMeasurementsForDayAsync(query, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
    }
}

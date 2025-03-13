using Energinet.DataHub.Measurements.Abstractions.Api.Queries;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.Client.Extensions.Options;
using Energinet.DataHub.Measurements.Client.Tests.Extensions;
using Energinet.DataHub.Measurements.Client.Tests.Fixtures;
using Microsoft.Extensions.DependencyInjection;
using Xunit.Abstractions;

namespace Energinet.DataHub.Measurements.Client.Tests;

public class MeasurementsClientTests : IAsyncLifetime
{
    private MeasurementsClientAppFixture Fixture { get; }

    private ServiceProvider ServiceProvider { get; }

    public MeasurementsClientTests(
        MeasurementsClientAppFixture fixture,
        ITestOutputHelper testOutputHelper)
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
    }

    public Task InitializeAsync()
    {
        throw new NotImplementedException();
    }

    public Task DisposeAsync()
    {
        throw new NotImplementedException();
    }

    [Fact]
    public async Task GetMeasurementsForDayAsync_WhenCalledWithValidQuery_ReturnsMeasurementDto()
    {
        // Arrange
        var client = ServiceProvider.GetRequiredService<IMeasurementsClient>();
        var query = new GetMeasurementsForDayQuery(
            "MeteringPointId",
            new DateTimeOffset(2025, 3, 1, 0, 0, 0, TimeSpan.Zero));

        // Act
        var result = await client.GetMeasurementsForDayAsync(query, CancellationToken.None);

        // Assert
        Assert.NotNull(result);
    }
}

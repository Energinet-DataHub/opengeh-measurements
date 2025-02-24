using AutoFixture.Xunit2;
using Energinet.DataHub.Measurements.Infrastructure.Handlers;

namespace Energinet.DataHub.Measurements.UnitTests.Handlers;

public class MeasurementsHandlerTests
{
    [Theory]
    [AutoData]
    public async Task GetMeasurementAsync_WhenCalledWithAnyId_ThenReturnsFortyTwo(
        string messageId,
        MeasurementsHandler sut)
    {
        // Act
        var actual = await sut.GetMeasurementAsync(messageId);

        // Assert
        Assert.Equal(42, actual);
    }
}

using System.Dynamic;
using AutoFixture.Xunit2;
using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Core.Databricks.SqlStatementExecution.Formats;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;
using Microsoft.Extensions.Options;
using Moq;
using NodaTime;
using Xunit;

namespace Energinet.DataHub.Measurements.UnitTests.Persistence;

public class MeasurementsRepositoryTests
{
    [Theory]
    [InlineAutoData]
    public async Task GetMeasurementAsync_WhenCalled_ReturnsMeasurement(
        Mock<DatabricksSqlWarehouseQueryExecutor> databricksSqlWarehouseQueryExecutorMock)
    {
        // Arrange
        const string meteringPointId = "1234567890";
        var from = Instant.FromUtc(2021, 1, 1, 0, 0);
        var to = Instant.FromUtc(2021, 1, 2, 0, 0);
        var raw = CreateMeasurementResults(10);
        databricksSqlWarehouseQueryExecutorMock
            .Setup(x => x.ExecuteStatementAsync(It.IsAny<GetMeasurementsQuery>(), It.IsAny<Format>(), It.IsAny<CancellationToken>()))
            .Returns(raw);
        var options = Options.Create(new DatabricksSchemaOptions { CatalogName = "catalog", SchemaName = "schema" });
        var sut = new MeasurementsRepository(databricksSqlWarehouseQueryExecutorMock.Object, options);

        // Act
        var actual = await sut.GetMeasurementsAsync(meteringPointId, from, to).ToListAsync();

        // Assert
        Assert.Equal(10, actual.Count);
    }

    private static async IAsyncEnumerable<ExpandoObject> CreateMeasurementResults(int count)
    {
        for (var i = 0; i < count; i++)
        {
            dynamic raw = new ExpandoObject();
            raw.metering_point_id = "123456789";
            raw.unit = "KWh";
            raw.observation_time = Instant.FromUtc(2021, 1, 1, 0, 0);
            raw.quantity = 42;
            raw.quality = "Measured";
            await Task.Yield();
            yield return raw;
        }
    }
}

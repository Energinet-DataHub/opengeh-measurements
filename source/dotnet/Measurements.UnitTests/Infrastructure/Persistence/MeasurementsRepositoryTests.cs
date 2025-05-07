using System.Dynamic;
using AutoFixture.Xunit2;
using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Core.Databricks.SqlStatementExecution.Formats;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Extensions;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;
using Microsoft.Extensions.Options;
using Moq;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Infrastructure.Persistence;

[UnitTest]
public class MeasurementsRepositoryTests
{
    [Theory]
    [InlineAutoData]
    public async Task GetByPeriod_WhenCalled_ReturnsMeasurements(
        Mock<DatabricksSqlWarehouseQueryExecutor> databricksSqlWarehouseQueryExecutorMock)
    {
        // Arrange
        const string meteringPointId = "1234567890123";
        var from = Instant.FromUtc(2021, 1, 1, 0, 0);
        var to = Instant.FromUtc(2021, 1, 2, 0, 0);
        var raw = CreateMeasurementResults(10);
        databricksSqlWarehouseQueryExecutorMock
            .Setup(x => x.ExecuteStatementAsync(It.IsAny<GetByPeriodQuery>(), It.IsAny<Format>(), It.IsAny<CancellationToken>()))
            .Returns(raw);
        var options = Options.Create(new DatabricksSchemaOptions { CatalogName = "catalog", SchemaName = "schema" });
        var sut = new MeasurementsRepository(databricksSqlWarehouseQueryExecutorMock.Object, options);

        // Act
        var actual = await sut.GetByPeriodAsync(meteringPointId, from, to).ToListAsync();

        // Assert
        Assert.Equal(10, actual.Count);
    }

    [Theory]
    [InlineAutoData]
    public async Task GetAggregatedByDateAsync_WhenCalled_ReturnsAggregatedMeasurements(
        Mock<DatabricksSqlWarehouseQueryExecutor> databricksSqlWarehouseQueryExecutorMock)
    {
        // Arrange
        const string meteringPointId = "1234567890123";
        var yearMonth = new YearMonth(2021, 1);
        var raw = CreateAggregatedMeasurementResults(yearMonth, 2);
        databricksSqlWarehouseQueryExecutorMock
            .Setup(x => x.ExecuteStatementAsync(It.IsAny<GetAggregatedByDateQuery>(), It.IsAny<Format>(), It.IsAny<CancellationToken>()))
            .Returns(raw);
        var options = Options.Create(new DatabricksSchemaOptions { CatalogName = "catalog", SchemaName = "schema" });
        var sut = new MeasurementsRepository(databricksSqlWarehouseQueryExecutorMock.Object, options);

        // Act
        var actual = await sut.GetAggregatedByDateAsync(meteringPointId, yearMonth).ToListAsync();
        var first = actual.First();

        // Assert
        Assert.Equal(2, actual.Count);
        Assert.Equal(1, first.PointCount);
        Assert.Equal(2, first.ObservationUpdates);
        Assert.Equal(42, first.Quantity);
        Assert.Equal("kWh", first.Units.Single());
        Assert.Equal("PT1H", first.Resolutions.Single());
        Assert.Equal("measured", first.Qualities.Single());
        Assert.Equal(Instant.FromUtc(2020, 12, 31, 23, 0), first.MinObservationTime);
        Assert.Equal(Instant.FromUtc(2021, 1, 1, 23, 0), first.MaxObservationTime);
    }

    [Theory]
    [InlineAutoData]
    public async Task GetAggregatedByMonthAsync_WhenCalled_ReturnsAggregatedMeasurements(
        Mock<DatabricksSqlWarehouseQueryExecutor> databricksSqlWarehouseQueryExecutorMock)
    {
        // Arrange
        const string meteringPointId = "1234567890123";
        var yearMonth = new YearMonth(2021, 1);
        var raw = CreateAggregatedMeasurementResults(yearMonth, 2);
        databricksSqlWarehouseQueryExecutorMock
            .Setup(x => x.ExecuteStatementAsync(It.IsAny<GetAggregatedByMonthQuery>(), It.IsAny<Format>(), It.IsAny<CancellationToken>()))
            .Returns(raw);
        var options = Options.Create(new DatabricksSchemaOptions { CatalogName = "catalog", SchemaName = "schema" });
        var sut = new MeasurementsRepository(databricksSqlWarehouseQueryExecutorMock.Object, options);

        // Act
        var actual = await sut.GetAggregatedByMonthAsync(meteringPointId, new Year(yearMonth.Year)).ToListAsync();
        var first = actual.First();

        // Assert
        Assert.Equal(2, actual.Count);
        Assert.Equal(1, first.PointCount);
        Assert.Equal(2, first.ObservationUpdates);
        Assert.Equal(42, first.Quantity);
        Assert.Equal("kWh", first.Units.Single());
        Assert.Equal("PT1H", first.Resolutions.Single());
        Assert.Equal("measured", first.Qualities.Single());
        Assert.Equal(Instant.FromUtc(2020, 12, 31, 23, 0), first.MinObservationTime);
        Assert.Equal(Instant.FromUtc(2021, 1, 1, 23, 0), first.MaxObservationTime);
    }

    [Theory]
    [InlineAutoData]
    public async Task GetAggregatedByYearAsync_WhenCalled_ReturnsAggregatedMeasurements(
        Mock<DatabricksSqlWarehouseQueryExecutor> databricksSqlWarehouseQueryExecutorMock)
    {
        // Arrange
        const string meteringPointId = "1234567890";
        var yearMonth = new YearMonth(2021, 1);
        var raw = CreateAggregatedMeasurementResults(yearMonth, 2);
        databricksSqlWarehouseQueryExecutorMock
            .Setup(x => x.ExecuteStatementAsync(It.IsAny<GetAggregatedByYearQuery>(), It.IsAny<Format>(), It.IsAny<CancellationToken>()))
            .Returns(raw);
        var options = Options.Create(new DatabricksSchemaOptions { CatalogName = "catalog", SchemaName = "schema" });
        var sut = new MeasurementsRepository(databricksSqlWarehouseQueryExecutorMock.Object, options);

        // Act
        var actual = await sut.GetAggregatedByYearAsync(meteringPointId).ToListAsync();
        var first = actual.First();

        // Assert
        Assert.Equal(2, actual.Count);
        Assert.Equal(1, first.PointCount);
        Assert.Equal(2, first.ObservationUpdates);
        Assert.Equal(42, first.Quantity);
        Assert.Equal("kWh", first.Units.Single());
        Assert.Equal("PT1H", first.Resolutions.Single());
        Assert.Equal("measured", first.Qualities.Single());
        Assert.Equal(Instant.FromUtc(2020, 12, 31, 23, 0), first.MinObservationTime);
        Assert.Equal(Instant.FromUtc(2021, 1, 1, 23, 0), first.MaxObservationTime);
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

    private static async IAsyncEnumerable<ExpandoObject> CreateAggregatedMeasurementResults(YearMonth yearMonth, int count)
    {
        var date = yearMonth.ToDateInterval().Start.ToDateTimeOffSet();

        for (var i = 0; i < count; i++)
        {
            dynamic raw = new ExpandoObject();
            raw.min_observation_time = date;
            raw.max_observation_time = date.AddDays(1);
            raw.aggregated_quantity = 42;
            raw.qualities = new[] { "measured" };
            raw.resolutions = new[] { "PT1H" };
            raw.units = new[] { "kWh" };
            raw.point_count = 1;
            raw.observation_updates = 2;
            await Task.Yield();
            yield return raw;

            date = date.AddDays(1);
        }
    }
}

using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Core.Databricks.SqlStatementExecution.Formats;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;
using Microsoft.Extensions.Options;
using NodaTime;

namespace Energinet.DataHub.Measurements.Infrastructure.Persistence;

public class MeasurementsRepository(
    DatabricksSqlWarehouseQueryExecutor databricksSqlWarehouseQueryExecutor,
    IOptions<DatabricksSchemaOptions> databricksSchemaOptions)
    : IMeasurementsRepository
{
    public async IAsyncEnumerable<MeasurementResult> GetByPeriodAsync(string meteringPointId, Instant from, Instant to)
    {
        var statement = new GetByPeriodQuery(meteringPointId, from, to, databricksSchemaOptions.Value);
        var rows = databricksSqlWarehouseQueryExecutor.ExecuteStatementAsync(statement, Format.ApacheArrow);

        await foreach (var row in rows)
            yield return new MeasurementResult(row);
    }

    public async IAsyncEnumerable<AggregatedMeasurementsResult> GetAggregatedByDateAsync(string meteringPointId, YearMonth yearMonth)
    {
        var statement = new GetAggregatedByDateQuery(meteringPointId, yearMonth, databricksSchemaOptions.Value);
        var rows = databricksSqlWarehouseQueryExecutor.ExecuteStatementAsync(statement, Format.ApacheArrow);

        await foreach (var row in rows)
            yield return new AggregatedMeasurementsResult(row);
    }

    public async IAsyncEnumerable<AggregatedMeasurementsResult> GetAggregatedByMonthAsync(string meteringPointId, Year year)
    {
        var statement = new GetAggregatedByMonthQuery(meteringPointId, year, databricksSchemaOptions.Value);
        var rows = databricksSqlWarehouseQueryExecutor.ExecuteStatementAsync(statement, Format.ApacheArrow);

        await foreach (var row in rows)
            yield return new AggregatedMeasurementsResult(row);
    }

    public async IAsyncEnumerable<AggregatedMeasurementsResult> GetAggregatedByYearAsync(string meteringPointId)
    {
        var statement = new GetAggregatedByYearQuery(meteringPointId, databricksSchemaOptions.Value);
        var rows = databricksSqlWarehouseQueryExecutor.ExecuteStatementAsync(statement, Format.ApacheArrow);

        await foreach (var row in rows)
            yield return new AggregatedMeasurementsResult(row);
    }
}

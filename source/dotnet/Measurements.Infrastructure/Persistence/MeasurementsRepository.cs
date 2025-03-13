using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Core.Databricks.SqlStatementExecution.Formats;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Application.Persistence;
using Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;
using Microsoft.Extensions.Options;
using NodaTime;

namespace Energinet.DataHub.Measurements.Infrastructure.Persistence;

public class MeasurementsRepository(
    DatabricksSqlWarehouseQueryExecutor databricksSqlWarehouseQueryExecutor,
    IOptions<DatabricksSchemaOptions> databricksSchemaOptions)
    : IMeasurementsRepository
{
    public async IAsyncEnumerable<MeasurementsResult> GetMeasurementsAsync(string meteringPointId, Instant from, Instant to)
    {
        var statement =
            new GetMeasurementsQuery(meteringPointId, from, to, databricksSchemaOptions.Value);

        var rows = databricksSqlWarehouseQueryExecutor
            .ExecuteStatementAsync(statement, Format.ApacheArrow);

        await foreach (var row in rows)
            yield return new MeasurementsResult(row);
    }
}

using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Extensions;

namespace Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;

public class GetAggregatedByYearQuery(string meteringPointId, Year year, DatabricksSchemaOptions databricksSchemaOptions)
    : DatabricksStatement
{
    private const string EuropeCopenhagenTimeZone = "Europe/Copenhagen";

    protected override string GetSqlStatement()
    {
        return AggregateSqlStatement.GetAggregateSqlStatement(
            databricksSchemaOptions.CatalogName,
            databricksSchemaOptions.SchemaName,
            CreateGroupByStatement());
    }

    protected override IReadOnlyCollection<QueryParameter> GetParameters()
    {
        var (startDate, endDate) = year.ToDateInterval();

        List<QueryParameter> parameters = [
            QueryParameter.Create(QueryParameterConstants.MeteringPointIdParameter, meteringPointId),
            QueryParameter.Create(QueryParameterConstants.ObservationTimeFromParameter, startDate.ToUtcString()),
            QueryParameter.Create(QueryParameterConstants.ObservationTimeToParameter, endDate.ToUtcString())
        ];

        return parameters;
    }

    private static string CreateGroupByStatement()
    {
        return $"{MeasurementsGoldConstants.MeteringPointIdColumnName}" +
               $", year(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{EuropeCopenhagenTimeZone}'))" +
               $", month(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{EuropeCopenhagenTimeZone}'))";
    }
}

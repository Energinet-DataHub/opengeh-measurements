using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Extensions;
using NodaTime;

namespace Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;

public class GetAggregatedByPeriodQuery(string meteringPointIds, Instant from, Instant to, DatabricksSchemaOptions databricksSchemaOptions)
    : DatabricksStatement
{
    private const string EuropeCopenhagenTimeZone = "Europe/Copenhagen";

    protected override string GetSqlStatement()
    {
        return AggregateSqlStatement.GetAggregateSqlStatement(
            databricksSchemaOptions.CatalogName,
            databricksSchemaOptions.SchemaName,
            CreateGroupByStatement(),
            GroupByMeteringPointAndObservationTime());
    }

    protected override IReadOnlyCollection<QueryParameter> GetParameters()
    {
        List<QueryParameter> parameters = [
            QueryParameter.Create(QueryParameterConstants.MeteringPointIdsParameter, meteringPointIds),
            QueryParameter.Create(QueryParameterConstants.ObservationTimeFromParameter, from.ToUtcString()),
            QueryParameter.Create(QueryParameterConstants.ObservationTimeToParameter, to.ToUtcString())
        ];

        return parameters;
    }

    private static string CreateGroupByStatement()
    {
        return $"select 42";
    }

    private static string GroupByMeteringPointAndObservationTime()
    {
        return $"{MeasurementsGoldConstants.MeteringPointIdColumnName}" +
               $", year(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}'))" +
               $", month(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}'))";
    }
}

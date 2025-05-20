using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Measurements.Application.Extensions.Options;

namespace Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;

public class GetAggregatedByYearQuery(string meteringPointId, DatabricksSchemaOptions databricksSchemaOptions)
    : DatabricksStatement
{
    protected override string GetSqlStatement()
    {
        return AggregateSqlStatement.GetAggregateSqlStatement(
            databricksSchemaOptions.CatalogName,
            databricksSchemaOptions.SchemaName,
            FilterOnMeteringPoint(),
            GroupByMeteringPointAndObservationTime());
    }

    protected override IReadOnlyCollection<QueryParameter> GetParameters()
    {
        List<QueryParameter> parameters = [
            QueryParameter.Create(QueryParameterConstants.MeteringPointIdParameter, meteringPointId),
        ];

        return parameters;
    }

    private static string FilterOnMeteringPoint()
    {
        return $"where {MeasurementsTableConstants.MeteringPointIdColumnName} = :{QueryParameterConstants.MeteringPointIdParameter}";
    }

    private static string GroupByMeteringPointAndObservationTime()
    {
        return $"{MeasurementsTableConstants.MeteringPointIdColumnName}" +
               $", year(from_utc_timestamp(cast({MeasurementsTableConstants.ObservationTimeColumnName} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}'))";
    }
}

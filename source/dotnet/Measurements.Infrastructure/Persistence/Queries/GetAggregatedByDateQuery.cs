using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Infrastructure.Extensions;
using NodaTime;

namespace Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;

public class GetAggregatedByDateQuery(string meteringPointId, YearMonth yearMonth, DatabricksSchemaOptions databricksSchemaOptions)
    : DatabricksStatement
{
    protected override string GetSqlStatement()
    {
        return AggregateSqlStatement.GetAggregateSqlStatement(
            databricksSchemaOptions.CatalogName,
            databricksSchemaOptions.SchemaName,
            FilterOnMeteringPointAndObservationTime(),
            GroupByMeteringPointAndObservationTime());
    }

    protected override IReadOnlyCollection<QueryParameter> GetParameters()
    {
        var (startDate, endDate) = yearMonth.ToDateInterval();

        List<QueryParameter> parameters = [
            QueryParameter.Create(QueryParameterConstants.MeteringPointIdParameter, meteringPointId),
            QueryParameter.Create(QueryParameterConstants.ObservationTimeFromParameter, startDate.ToUtcString()),
            QueryParameter.Create(QueryParameterConstants.ObservationTimeToParameter, endDate.ToUtcString())
        ];

        return parameters;
    }

    private static string FilterOnMeteringPointAndObservationTime()
    {
        return $"where {MeasurementsGoldConstants.MeteringPointIdColumnName} = :{QueryParameterConstants.MeteringPointIdParameter} " +
               $"and {MeasurementsGoldConstants.ObservationTimeColumnName} >= :{QueryParameterConstants.ObservationTimeFromParameter} " +
               $"and {MeasurementsGoldConstants.ObservationTimeColumnName} < :{QueryParameterConstants.ObservationTimeToParameter}";
    }

    private static string GroupByMeteringPointAndObservationTime()
    {
        return $"{MeasurementsGoldConstants.MeteringPointIdColumnName}" +
               $", year(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}'))" +
               $", month(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}'))" +
               $", dayofmonth(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}'))";
    }
}

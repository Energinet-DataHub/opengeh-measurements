using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Extensions;
using NodaTime;

namespace Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;

public class GetAggregatedByPeriodQuery(string meteringPointIds, Instant from, Instant to, Aggregation aggregation, DatabricksSchemaOptions databricksSchemaOptions)
    : DatabricksStatement
{
    protected override string GetSqlStatement()
    {
        return AggregateSqlStatement.GetAggregatedByPeriodSqlStatement(
            databricksSchemaOptions.CatalogName,
            databricksSchemaOptions.SchemaName,
            CreateWhereStatement(),
            CreateGroupKeyStatement(aggregation),
            CreateGroupByStatement(aggregation));
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

    private static string CreateGroupKeyStatement(Aggregation aggregation)
    {
        return aggregation switch
        {
            Aggregation.Quarter or Aggregation.Hour or Aggregation.Day =>
                $"cast(date(from_utc_timestamp(cast({AggregatedQueryConstants.MinObservationTime} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}')) as string)",
            Aggregation.Month =>
                $"substring(date(from_utc_timestamp(cast({AggregatedQueryConstants.MinObservationTime} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}')), 0, 7)",
            Aggregation.Year =>
                $"substring(date(from_utc_timestamp(cast({AggregatedQueryConstants.MinObservationTime} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}')), 0, 4)",
            _ => throw new ArgumentOutOfRangeException(aggregation.ToString()),
        };
    }

    private static string CreateWhereStatement()
    {
        return $"where {MeasurementsTableConstants.MeteringPointIdColumnName} in (:{QueryParameterConstants.MeteringPointIdsParameter}) " +
               $"and {MeasurementsTableConstants.ObservationTimeColumnName} >= :{QueryParameterConstants.ObservationTimeFromParameter} " +
               $"and {MeasurementsTableConstants.ObservationTimeColumnName} < :{QueryParameterConstants.ObservationTimeToParameter}";
    }

    private static string CreateGroupByStatement(Aggregation aggregation)
    {
        var windowTimeStatement = aggregation switch
        {
            Aggregation.Quarter => "15 MINUTES",
            Aggregation.Hour => "1 HOUR",
            _ => string.Empty,
        };

        return $"{MeasurementsTableConstants.MeteringPointIdColumnName}" +
       $", year(from_utc_timestamp(cast({MeasurementsTableConstants.ObservationTimeColumnName} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}'))" +
       (aggregation <= Aggregation.Month
           ? $", month(from_utc_timestamp(cast({MeasurementsTableConstants.ObservationTimeColumnName} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}'))"
           : string.Empty) +
       (aggregation <= Aggregation.Day
           ? $", dayofmonth(from_utc_timestamp(cast({MeasurementsTableConstants.ObservationTimeColumnName} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}'))"
           : string.Empty) +
       (aggregation <= Aggregation.Hour
           ? $", window(cast({MeasurementsTableConstants.ObservationTimeColumnName} as timestamp), '{windowTimeStatement}')"
           : string.Empty);
    }
}

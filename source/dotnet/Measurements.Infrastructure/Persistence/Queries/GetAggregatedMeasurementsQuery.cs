using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Infrastructure.Extensions;
using NodaTime;

namespace Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;

public class GetAggregatedMeasurementsQuery : DatabricksStatement
{
    private const string EuropeCopenhagenTimeZone = "Europe/Copenhagen";
    private readonly string _meteringPointId;
    private readonly YearMonth _yearMonth;
    private readonly DatabricksSchemaOptions _databricksSchemaOptions;

    public GetAggregatedMeasurementsQuery(string meteringPointId, YearMonth yearMonth, DatabricksSchemaOptions databricksSchemaOptions)
    {
        _meteringPointId = meteringPointId;
        _yearMonth = yearMonth;
        _databricksSchemaOptions = databricksSchemaOptions;
    }

    protected override string GetSqlStatement()
    {
        const string AggregatedQuantity = "aggregated_quantity";
        const string MinObservationTime = "min_observation_time";
        const string MaxObservationTime = "max_observation_time";
        const string Qualities = "qualities";
        const string Resolutions = "resolutions";
        const string PointCount = "point_count";

        var (startDate, endDate) = _yearMonth.ToDateInterval();

        const string groupByStatement = $"{MeasurementsGoldConstants.MeteringPointIdColumnName}" +
                                        $", year(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{EuropeCopenhagenTimeZone}'))" +
                                        $", month(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{EuropeCopenhagenTimeZone}'))" +
                                        $", dayofmonth(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{EuropeCopenhagenTimeZone}'))";

        return
            $"with most_recent as (" +
            $"select row_number() over (partition by {MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName} order by {MeasurementsGoldConstants.TransactionCreationDatetimeColumnName} desc) as row, " +
            $"{MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.UnitColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName}, {MeasurementsGoldConstants.QuantityColumnName}, {MeasurementsGoldConstants.QualityColumnName}, {MeasurementsGoldConstants.ResolutionColumnName}, {MeasurementsGoldConstants.IsCancelledColumnName} " +
            $"from {_databricksSchemaOptions.CatalogName}.{_databricksSchemaOptions.SchemaName}.{MeasurementsGoldConstants.TableName} " +
            $"where {MeasurementsGoldConstants.MeteringPointIdColumnName} = '{_meteringPointId}' " +
            $"and {MeasurementsGoldConstants.ObservationTimeColumnName} >= '{startDate.ToUtcString()}' " +
            $"and {MeasurementsGoldConstants.ObservationTimeColumnName} < '{endDate.ToUtcString()}' " +
            $") " +
            $"select {MeasurementsGoldConstants.MeteringPointIdColumnName}, " +
            $"min({MeasurementsGoldConstants.ObservationTimeColumnName}) as {MinObservationTime}, " +
            $"max({MeasurementsGoldConstants.ObservationTimeColumnName}) as {MaxObservationTime}, " +
            $"sum({MeasurementsGoldConstants.QuantityColumnName}) as {AggregatedQuantity}, " +
            $"array_agg(distinct({MeasurementsGoldConstants.QualityColumnName})) as {Qualities}, " +
            $"array_agg(distinct({MeasurementsGoldConstants.ResolutionColumnName})) as {Resolutions}, " +
            $"count({MeasurementsGoldConstants.ObservationTimeColumnName}) as {PointCount} " +
            $"from most_recent " +
            $"where row = 1 " +
            $"and not {MeasurementsGoldConstants.IsCancelledColumnName} " +
            $"group by {groupByStatement} " +
            $"order by {MinObservationTime}";
    }
}

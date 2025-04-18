﻿using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
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
        return
            $"with most_recent as (" +
            $"select row_number() over (partition by {MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName} order by {MeasurementsGoldConstants.TransactionCreationDatetimeColumnName} desc) as row, " +
            $"count(*) over (partition by {MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName}) as row_count, " +
            $"{MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.UnitColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName}, {MeasurementsGoldConstants.QuantityColumnName}, {MeasurementsGoldConstants.QualityColumnName}, {MeasurementsGoldConstants.ResolutionColumnName}, {MeasurementsGoldConstants.IsCancelledColumnName} " +
            $"from {_databricksSchemaOptions.CatalogName}.{_databricksSchemaOptions.SchemaName}.{MeasurementsGoldConstants.TableName} " +
            $"where {MeasurementsGoldConstants.MeteringPointIdColumnName} = :{QueryParameterConstants.MeteringPointIdParameter} " +
            $"and {MeasurementsGoldConstants.ObservationTimeColumnName} >= :{QueryParameterConstants.ObservationTimeFromParameter} " +
            $"and {MeasurementsGoldConstants.ObservationTimeColumnName} < :{QueryParameterConstants.ObservationTimeToParameter} " +
            $") " +
            $"select {MeasurementsGoldConstants.MeteringPointIdColumnName}, " +
            $"min({MeasurementsGoldConstants.ObservationTimeColumnName}) as {AggregatedMeasurementsConstants.MinObservationTime}, " +
            $"max({MeasurementsGoldConstants.ObservationTimeColumnName}) as {AggregatedMeasurementsConstants.MaxObservationTime}, " +
            $"sum({MeasurementsGoldConstants.QuantityColumnName}) as {AggregatedMeasurementsConstants.AggregatedQuantity}, " +
            $"array_agg(distinct({MeasurementsGoldConstants.QualityColumnName})) as {AggregatedMeasurementsConstants.Qualities}, " +
            $"array_agg(distinct({MeasurementsGoldConstants.ResolutionColumnName})) as {AggregatedMeasurementsConstants.Resolutions}, " +
            $"array_agg(distinct({MeasurementsGoldConstants.UnitColumnName})) as {AggregatedMeasurementsConstants.Units}, " +
            $"count({MeasurementsGoldConstants.ObservationTimeColumnName}) as {AggregatedMeasurementsConstants.PointCount}, " +
            $"max(row_count) as {AggregatedMeasurementsConstants.ObservationUpdates} " +
            $"from most_recent " +
            $"where row = 1 " +
            $"and not {MeasurementsGoldConstants.IsCancelledColumnName} " +
            $"group by {CreateGroupByStatement()} " +
            $"order by {AggregatedMeasurementsConstants.MinObservationTime}";
    }

    protected override IReadOnlyCollection<QueryParameter> GetParameters()
    {
        var (startDate, endDate) = _yearMonth.ToDateInterval();

        List<QueryParameter> parameters = [
            QueryParameter.Create(QueryParameterConstants.MeteringPointIdParameter, _meteringPointId),
            QueryParameter.Create(QueryParameterConstants.ObservationTimeFromParameter, startDate.ToUtcString()),
            QueryParameter.Create(QueryParameterConstants.ObservationTimeToParameter, endDate.ToUtcString())
        ];

        return parameters;
    }

    private static string CreateGroupByStatement()
    {
        return $"{MeasurementsGoldConstants.MeteringPointIdColumnName}" +
               $", year(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{EuropeCopenhagenTimeZone}'))" +
               $", month(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{EuropeCopenhagenTimeZone}'))" +
               $", dayofmonth(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{EuropeCopenhagenTimeZone}'))";
    }
}

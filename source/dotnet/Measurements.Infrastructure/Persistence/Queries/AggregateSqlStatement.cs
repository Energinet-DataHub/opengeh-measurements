namespace Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;

public static class AggregateSqlStatement
{
    public static string GetAggregateSqlStatement(string catalogName, string schemaName, string whereStatement, string groupByStatement)
    {
        return
            $"with most_recent as (" +
            $"select row_number() over (partition by {MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName} order by {MeasurementsGoldConstants.TransactionCreationDatetimeColumnName} desc) as row, " +
            $"count(*) over (partition by {MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName}) as row_count, " +
            $"{MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.UnitColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName}, {MeasurementsGoldConstants.QuantityColumnName}, {MeasurementsGoldConstants.QualityColumnName}, {MeasurementsGoldConstants.ResolutionColumnName}, {MeasurementsGoldConstants.IsCancelledColumnName} " +
            $"from {catalogName}.{schemaName}.{MeasurementsGoldConstants.TableName} " +
            $"{whereStatement} " +
            $") " +
            $"select {MeasurementsGoldConstants.MeteringPointIdColumnName}, " +
            $"min({MeasurementsGoldConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.MinObservationTime}, " +
            $"max({MeasurementsGoldConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.MaxObservationTime}, " +
            $"sum({MeasurementsGoldConstants.QuantityColumnName}) as {AggregatedQueryConstants.AggregatedQuantity}, " +
            $"array_agg(distinct({MeasurementsGoldConstants.QualityColumnName})) as {AggregatedQueryConstants.Qualities}, " +
            $"array_agg(distinct({MeasurementsGoldConstants.ResolutionColumnName})) as {AggregatedQueryConstants.Resolutions}, " +
            $"array_agg(distinct({MeasurementsGoldConstants.UnitColumnName})) as {AggregatedQueryConstants.Units}, " +
            $"count({MeasurementsGoldConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.PointCount}, " +
            $"max(row_count) as {AggregatedQueryConstants.ObservationUpdates} " +
            $"from most_recent " +
            $"where row = 1 " +
            $"and not {MeasurementsGoldConstants.IsCancelledColumnName} " +
            $"group by {groupByStatement} " +
            $"order by {AggregatedQueryConstants.MinObservationTime}";
    }
}

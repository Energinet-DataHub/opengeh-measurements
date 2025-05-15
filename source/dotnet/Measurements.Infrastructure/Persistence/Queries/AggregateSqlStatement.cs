namespace Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;

public static class AggregateSqlStatement
{
    public static string GetAggregateSqlStatement(string catalogName, string schemaName, string whereStatement, string groupByStatement)
    {
        return
            $"with most_recent as (" +
            $"select row_number() over (partition by {MeasurementsTableConstants.MeteringPointIdColumnName}, {MeasurementsTableConstants.ObservationTimeColumnName} order by {MeasurementsTableConstants.TransactionCreationDatetimeColumnName} desc) as row, " +
            $"count(*) over (partition by {MeasurementsTableConstants.MeteringPointIdColumnName}, {MeasurementsTableConstants.ObservationTimeColumnName}) as row_count, " +
            $"{MeasurementsTableConstants.MeteringPointIdColumnName}, {MeasurementsTableConstants.UnitColumnName}, {MeasurementsTableConstants.ObservationTimeColumnName}, {MeasurementsTableConstants.QuantityColumnName}, {MeasurementsTableConstants.QualityColumnName}, {MeasurementsTableConstants.ResolutionColumnName}, {MeasurementsTableConstants.IsCancelledColumnName} " +
            $"from {catalogName}.{schemaName}.{MeasurementsTableConstants.Name} " +
            $"{whereStatement} " +
            $") " +
            $"select {MeasurementsTableConstants.MeteringPointIdColumnName}, " +
            $"min({MeasurementsTableConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.MinObservationTime}, " +
            $"max({MeasurementsTableConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.MaxObservationTime}, " +
            $"sum({MeasurementsTableConstants.QuantityColumnName}) as {AggregatedQueryConstants.AggregatedQuantity}, " +
            $"array_agg(distinct({MeasurementsTableConstants.QualityColumnName})) as {AggregatedQueryConstants.Qualities}, " +
            $"array_agg(distinct({MeasurementsTableConstants.ResolutionColumnName})) as {AggregatedQueryConstants.Resolutions}, " +
            $"array_agg(distinct({MeasurementsTableConstants.UnitColumnName})) as {AggregatedQueryConstants.Units}, " +
            $"count({MeasurementsTableConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.PointCount}, " +
            $"max(row_count) as {AggregatedQueryConstants.ObservationUpdates} " +
            $"from most_recent " +
            $"where row = 1 " +
            $"and not {MeasurementsTableConstants.IsCancelledColumnName} " +
            $"group by {groupByStatement} " +
            $"order by {AggregatedQueryConstants.MinObservationTime}";
    }

    public static string GetAggregatedByPeriodSqlStatement(string catalogName, string schemaName, string whereStatement, string aggregationGroupKeyStatement, string groupByStatement)
    {
        return $"select " +
               $"{MeasurementsTableConstants.MeteringPointIdColumnName}, " +
               $"min({MeasurementsTableConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.MinObservationTime}, " +
               $"max({MeasurementsTableConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.MaxObservationTime}, " +
               $"sum({MeasurementsTableConstants.QuantityColumnName}) as {AggregatedQueryConstants.AggregatedQuantity}, " +
               $"array_agg(distinct({MeasurementsTableConstants.QualityColumnName})) as {AggregatedQueryConstants.Qualities}, " +
               $"array_agg(distinct({MeasurementsTableConstants.ResolutionColumnName})) as {AggregatedQueryConstants.Resolutions}, " +
               $"{aggregationGroupKeyStatement} as {AggregatedQueryConstants.AggregationGroupKey} " +
               $"from {catalogName}.{schemaName}.{MeasurementsTableConstants.Name} " +
               $"where {whereStatement} " +
               $"group by {groupByStatement} " +
               $"order by {MeasurementsTableConstants.MeteringPointIdColumnName}, {AggregatedQueryConstants.MinObservationTime}";
    }
}

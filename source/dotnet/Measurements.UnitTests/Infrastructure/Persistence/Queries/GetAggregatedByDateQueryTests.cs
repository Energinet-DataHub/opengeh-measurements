﻿using System.ComponentModel.DataAnnotations;
using AutoFixture.Xunit2;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Infrastructure.Persistence.Queries;

[UnitTest]
public class GetAggregatedByDateQueryTests
{
    [Theory]
    [AutoData]
    public void ToString_Returns_ExpectedResult(string meteringPointId, [Range(-9998, 9999)] int year, [Range(1, 12)] int month)
    {
        // Arrange
        var yearMonth = new YearMonth(year, month);
        var databricksSchemaOptions = new DatabricksSchemaOptions { CatalogName = "spark_catalog", SchemaName = "schema_name" };
        var expected = CreateExpectedQuery(databricksSchemaOptions);
        var getAggregatedByMonthQuery = new GetAggregatedByDateQuery(meteringPointId, yearMonth, databricksSchemaOptions);

        // Act
        var actual = getAggregatedByMonthQuery.ToString();

        // Assert
        Assert.Equal(expected, actual);
    }

    private static string CreateExpectedQuery(DatabricksSchemaOptions databricksSchemaOptions)
    {
        return $"with most_recent as (" +
               $"select row_number() over (partition by {MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName} order by {MeasurementsGoldConstants.TransactionCreationDatetimeColumnName} desc) as row, " +
               $"count(*) over (partition by {MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName}) as row_count, " +
               $"{MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.UnitColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName}, {MeasurementsGoldConstants.QuantityColumnName}, {MeasurementsGoldConstants.QualityColumnName}, {MeasurementsGoldConstants.ResolutionColumnName}, {MeasurementsGoldConstants.IsCancelledColumnName} " +
               $"from {databricksSchemaOptions.CatalogName}.{databricksSchemaOptions.SchemaName}.{MeasurementsGoldConstants.TableName} " +
               $"where {MeasurementsGoldConstants.MeteringPointIdColumnName} = :{QueryParameterConstants.MeteringPointIdParameter} " +
               $"and {MeasurementsGoldConstants.ObservationTimeColumnName} >= :{QueryParameterConstants.ObservationTimeFromParameter} " +
               $"and {MeasurementsGoldConstants.ObservationTimeColumnName} < :{QueryParameterConstants.ObservationTimeToParameter} " +
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
               $"group by {CreateGroupByStatement()} " +
               $"order by {AggregatedQueryConstants.MinObservationTime}";
    }

    private static string CreateGroupByStatement()
    {
        return $"{MeasurementsGoldConstants.MeteringPointIdColumnName}" +
               $", year(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}'))" +
               $", month(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}'))" +
               $", dayofmonth(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{TimeZoneConstants.EuropeCopenhagenTimeZone}'))";
    }
}

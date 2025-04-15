using System.ComponentModel.DataAnnotations;
using AutoFixture.Xunit2;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;
using NodaTime;
using Xunit;

namespace Energinet.DataHub.Measurements.UnitTests.Infrastructure.Persistence.Queries;

public class GetAggregatedMeasurementsQueryTests
{
    [Theory]
    [AutoData]
    public void ToString_Returns_ExpectedResult(string meteringPointId, [Range(-9998, 9999)]int year, [Range(1, 12)]int month)
    {
        // Arrange
        var yearMonth = new YearMonth(year, month);
        var databricksSchemaOptions = new DatabricksSchemaOptions { CatalogName = "spark_catalog", SchemaName = "schema_name" };
        var expected = CreateExpectedQuery(databricksSchemaOptions);
        var getAggregatedMeasurementsQuery = new GetAggregatedMeasurementsQuery(meteringPointId, yearMonth, databricksSchemaOptions);

        // Act
        var actual = getAggregatedMeasurementsQuery.ToString();

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

    private static string CreateGroupByStatement()
    {
        const string europeCopenhagenTimeZone = "Europe/Copenhagen";

        return $"{MeasurementsGoldConstants.MeteringPointIdColumnName}" +
               $", year(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{europeCopenhagenTimeZone}'))" +
               $", month(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{europeCopenhagenTimeZone}'))" +
               $", dayofmonth(from_utc_timestamp(cast({MeasurementsGoldConstants.ObservationTimeColumnName} as timestamp), '{europeCopenhagenTimeZone}'))";
    }
}

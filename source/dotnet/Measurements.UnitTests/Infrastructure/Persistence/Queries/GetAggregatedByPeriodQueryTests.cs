using AutoFixture.Xunit2;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Domain;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Infrastructure.Persistence.Queries;

[UnitTest]
public class GetAggregatedByPeriodQueryTests
{
    [Theory]
    [AutoData]
    public void ToString_Returns_ExpectedResult(
        string meteringPointIds,
        Instant from,
        Instant to,
        Aggregation aggregation)
    {
        // Arrange
        var databricksSchemaOptions =
            new DatabricksSchemaOptions { CatalogName = "spark_catalog", SchemaName = "schema_name" };
        var expected = CreateExpectedQuery(databricksSchemaOptions, aggregation);
        var getAggregatedByPeriodQuery =
            new GetAggregatedByPeriodQuery(meteringPointIds, from, to, aggregation, databricksSchemaOptions);

        // Act
        var actual = getAggregatedByPeriodQuery.ToString();

        // Assert
        Assert.Equal(expected, actual);
    }

    private static string CreateExpectedQuery(DatabricksSchemaOptions databricksSchemaOptions, Aggregation aggregation)
    {
        return $"with most_recent as (" +
               $"select row_number() over (partition by {MeasurementsTableConstants.MeteringPointIdColumnName}, {MeasurementsTableConstants.ObservationTimeColumnName} order by {MeasurementsTableConstants.TransactionCreationDatetimeColumnName} desc) as row, " +
               $"{MeasurementsTableConstants.MeteringPointIdColumnName}, {MeasurementsTableConstants.ResolutionColumnName}, {MeasurementsTableConstants.ObservationTimeColumnName}, {MeasurementsTableConstants.QuantityColumnName}, {MeasurementsTableConstants.QualityColumnName}, {MeasurementsTableConstants.IsCancelledColumnName} " +
               $"from {databricksSchemaOptions.CatalogName}.{databricksSchemaOptions.SchemaName}.{MeasurementsTableConstants.Name} " +
               $"{CreateWhereStatement()} " +
               $") " +
               $"select " +
               $"{MeasurementsTableConstants.MeteringPointIdColumnName}, " +
               $"{MeasurementsTableConstants.ResolutionColumnName}, " +
               $"min({MeasurementsTableConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.MinObservationTime}, " +
               $"max({MeasurementsTableConstants.ObservationTimeColumnName}) as {AggregatedQueryConstants.MaxObservationTime}, " +
               $"sum({MeasurementsTableConstants.QuantityColumnName}) as {AggregatedQueryConstants.AggregatedQuantity}, " +
               $"array_agg(distinct({MeasurementsTableConstants.QualityColumnName})) as {AggregatedQueryConstants.Qualities}, " +
               $"{CreateAggregationGroupKeyStatement(aggregation)} as {AggregatedQueryConstants.AggregationGroupKey} " +
               $"from most_recent " +
               $"where row = 1 " +
               $"and not {MeasurementsTableConstants.IsCancelledColumnName} " +
               $"group by {CreateGroupByStatement(aggregation)} " +
               $"order by {MeasurementsTableConstants.MeteringPointIdColumnName}, {AggregatedQueryConstants.MinObservationTime}";
    }

    private static string CreateAggregationGroupKeyStatement(Aggregation aggregation)
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

    private static string CreateGroupByStatement(Aggregation aggregation)
    {
        var windowTimeStatement = aggregation switch
        {
            Aggregation.Quarter => "15 MINUTES",
            Aggregation.Hour => "1 HOUR",
            _ => string.Empty,
        };

        return $"{MeasurementsTableConstants.MeteringPointIdColumnName}" +
               $", {MeasurementsTableConstants.ResolutionColumnName}" +
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

    private static string CreateWhereStatement()
    {
        return $"where {MeasurementsTableConstants.MeteringPointIdColumnName} in (:{QueryParameterConstants.MeteringPointIdsParameter}) " +
               $"and {MeasurementsTableConstants.ObservationTimeColumnName} >= :{QueryParameterConstants.ObservationTimeFromParameter} " +
               $"and {MeasurementsTableConstants.ObservationTimeColumnName} < :{QueryParameterConstants.ObservationTimeToParameter}";
    }
}

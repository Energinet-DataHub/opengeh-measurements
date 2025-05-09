﻿using AutoFixture.Xunit2;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using Energinet.DataHub.Measurements.Infrastructure.Persistence.Queries;
using NodaTime;
using Xunit;
using Xunit.Categories;

namespace Energinet.DataHub.Measurements.UnitTests.Infrastructure.Persistence.Queries;

[UnitTest]
public class GetByPeriodQueryTests
{
    [Theory]
    [AutoData]
    public void ToString_Returns_ExpectedResult(string meteringPointId, Instant startDate, Instant endDate)
    {
        // Arrange
        var databricksSchemaOptions = new DatabricksSchemaOptions { CatalogName = "spark_catalog", SchemaName = "schema_name" };
        var expectedResult = CreateExpectedQuery(databricksSchemaOptions);
        var getByPeriodQuery = new GetByPeriodQuery(meteringPointId, startDate, endDate, databricksSchemaOptions);

        // Act
        var result = getByPeriodQuery.ToString();

        // Assert
        Assert.Equal(expectedResult, result);
    }

    private static string CreateExpectedQuery(DatabricksSchemaOptions databricksSchemaOptions)
    {
        return $"with most_recent as (" +
               $"select row_number() over (partition by {MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName} order by {MeasurementsGoldConstants.TransactionCreationDatetimeColumnName} desc) as row, " +
               $"{MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.UnitColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName}, {MeasurementsGoldConstants.QuantityColumnName}, {MeasurementsGoldConstants.QualityColumnName}, {MeasurementsGoldConstants.ResolutionColumnName}, {MeasurementsGoldConstants.IsCancelledColumnName}, {MeasurementsGoldConstants.CreatedColumnName}, {MeasurementsGoldConstants.TransactionCreationDatetimeColumnName} " +
               $"from {databricksSchemaOptions.CatalogName}.{databricksSchemaOptions.SchemaName}.{MeasurementsGoldConstants.TableName} " +
               $"where {MeasurementsGoldConstants.MeteringPointIdColumnName} = :{QueryParameterConstants.MeteringPointIdParameter} " +
               $"and {MeasurementsGoldConstants.ObservationTimeColumnName} >= :{QueryParameterConstants.ObservationTimeFromParameter} " +
               $"and {MeasurementsGoldConstants.ObservationTimeColumnName} < :{QueryParameterConstants.ObservationTimeToParameter} " +
               $") " +
               $"select {MeasurementsGoldConstants.MeteringPointIdColumnName}, {MeasurementsGoldConstants.UnitColumnName}, {MeasurementsGoldConstants.ObservationTimeColumnName}, {MeasurementsGoldConstants.QuantityColumnName}, {MeasurementsGoldConstants.QualityColumnName}, {MeasurementsGoldConstants.ResolutionColumnName}, {MeasurementsGoldConstants.CreatedColumnName}, {MeasurementsGoldConstants.TransactionCreationDatetimeColumnName} " +
               $"from most_recent " +
               $"where not {MeasurementsGoldConstants.IsCancelledColumnName} " +
               $"order by {MeasurementsGoldConstants.ObservationTimeColumnName}";
    }
}

﻿using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Core.FunctionApp.TestCommon.Configuration;
using Energinet.DataHub.Core.FunctionApp.TestCommon.Databricks;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using Energinet.DataHub.Measurements.WebApi;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using NodaTime;

namespace Energinet.DataHub.Measurements.Client.Tests.Fixtures;

public class MeasurementsClientFixture : WebApplicationFactory<Program>, IAsyncLifetime
{
    public HttpClient HttpClient { get; }

    private DatabricksSchemaManager DatabricksSchemaManager { get; set; }

    private IntegrationTestConfiguration IntegrationTestConfiguration { get; }

    public MeasurementsClientFixture()
    {
        IntegrationTestConfiguration = new IntegrationTestConfiguration();
        DatabricksSchemaManager = new DatabricksSchemaManager(
            new HttpClientFactory(),
            IntegrationTestConfiguration.DatabricksSettings,
            "mmcore_measurementsapi");
        HttpClient = CreateClient();
    }

    public async Task InitializeAsync()
    {
        await DatabricksSchemaManager.CreateSchemaAsync();
        await DatabricksSchemaManager.CreateTableAsync(MeasurementsGoldConstants.TableName, CreateColumnDefinitions());
        await DatabricksSchemaManager.InsertAsync(MeasurementsGoldConstants.TableName, CreateRows());
    }

    public new async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await DatabricksSchemaManager.DropSchemaAsync();
        HttpClient.Dispose();
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseSetting($"{DatabricksSqlStatementOptions.DatabricksOptions}:{nameof(DatabricksSqlStatementOptions.WorkspaceUrl)}", IntegrationTestConfiguration.DatabricksSettings.WorkspaceUrl);
        builder.UseSetting($"{DatabricksSqlStatementOptions.DatabricksOptions}:{nameof(DatabricksSqlStatementOptions.WorkspaceToken)}", IntegrationTestConfiguration.DatabricksSettings.WorkspaceAccessToken);
        builder.UseSetting($"{DatabricksSqlStatementOptions.DatabricksOptions}:{nameof(DatabricksSqlStatementOptions.WarehouseId)}", IntegrationTestConfiguration.DatabricksSettings.WarehouseId);
        builder.UseSetting($"{DatabricksSchemaOptions.SectionName}:{nameof(DatabricksSchemaOptions.SchemaName)}", DatabricksSchemaManager.SchemaName);
        builder.UseSetting($"{DatabricksSchemaOptions.SectionName}:{nameof(DatabricksSchemaOptions.CatalogName)}", "hive_metastore");
    }

    private static Dictionary<string, (string DataType, bool IsNullable)> CreateColumnDefinitions() =>
        new()
        {
            { MeasurementsGoldConstants.MeteringPointIdColumnName, ("STRING", false) },
            { MeasurementsGoldConstants.UnitColumnName, ("STRING", false) },
            { MeasurementsGoldConstants.ObservationTimeColumnName, ("TIMESTAMP", false) },
            { MeasurementsGoldConstants.QuantityColumnName, ("DECIMAL(18, 6)", false) },
            { MeasurementsGoldConstants.QualityColumnName, ("STRING", false) },
            { MeasurementsGoldConstants.TransactionCreationDatetimeColumnName, ("TIMESTAMP", false) },
        };

    private static List<IEnumerable<string>> CreateRows()
    {
        var dates = new[]
        {
            new LocalDate(2023, 1, 2),
            new LocalDate(2023, 1, 3),
            new LocalDate(2023, 1, 4),
            new LocalDate(2023, 1, 5),
            new LocalDate(2023, 1, 6),
            new LocalDate(2023, 1, 7),
            new LocalDate(2023, 1, 8),
            new LocalDate(2023, 6, 15),
        };

        return [.. dates.SelectMany(CreateRow)];
    }

    private static IEnumerable<IEnumerable<string>> CreateRow(LocalDate observationDate)
    {
        var observationTime = observationDate.ToUtcDateTimeOffset();

        return Enumerable.Range(0, 24).Select(i => new[]
        {
            "'1234567890'",
            "'kwh'",
            $"'{observationTime.AddHours(i).ToFormattedString()}'",
            $"{i}.4",
            "'measured'",
            "'2025-03-12T03:40:55Z'",
        });
    }
}

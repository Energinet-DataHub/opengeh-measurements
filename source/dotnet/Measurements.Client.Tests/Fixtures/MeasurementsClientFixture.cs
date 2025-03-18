using System.Globalization;
using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
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
    public DatabricksSchemaManager DatabricksSchemaManager { get; set; }

    public IntegrationTestConfiguration IntegrationTestConfiguration { get; }

    public HttpClient HttpClient { get; }

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
        await DatabricksSchemaManager.InsertAsync(MeasurementsGoldConstants.TableName, CreateRows(new LocalDate(2025, 1, 2)));
        await DatabricksSchemaManager.InsertAsync(MeasurementsGoldConstants.TableName, CreateRows(new LocalDate(2025, 6, 15)));
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
        };

    private static IEnumerable<IEnumerable<string>> CreateRows(LocalDate observationDate)
    {
        var obsTime = observationDate.ToUtcDateTimeOffset();
        for (var i = 0; i <= 23; i++)
        {
            yield return new List<string> { "'1234567890'", "'kwh'", $"'{obsTime.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture)}'", $"{i}.4", "'measured'" };
            obsTime = obsTime.AddHours(1);
        }
    }
}

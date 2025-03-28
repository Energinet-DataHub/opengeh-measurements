using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Core.FunctionApp.TestCommon.Configuration;
using Energinet.DataHub.Core.FunctionApp.TestCommon.Databricks;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.VisualStudio.TestPlatform.TestHost;
using NodaTime;
using Xunit;

namespace Energinet.DataHub.Measurements.Client.IntegrationTests.Fixture;

public class MeasurementsClientFixture : WebApplicationFactory<Program>, IAsyncLifetime
{
    public const string ApplicationIdUri = "https://management.azure.com";
    public const string Issuer = "https://sts.windows.net/f7619355-6c67-4100-9a78-1847f30742e2/";
    public const string CatalogName = "hive_metastore";

    private DatabricksSchemaManager DatabricksSchemaManager { get; set; }

    private IntegrationTestConfiguration IntegrationTestConfiguration { get; }

    public MeasurementsClientFixture()
    {
        IntegrationTestConfiguration = new IntegrationTestConfiguration();
        DatabricksSchemaManager = new DatabricksSchemaManager(
            new HttpClientFactory(),
            IntegrationTestConfiguration.DatabricksSettings,
            "mmcore_measurementsapi");
    }

    public async Task InitializeAsync()
    {
        await DatabricksSchemaManager.CreateSchemaAsync();
        await DatabricksSchemaManager.CreateTableAsync(MeasurementsGoldConstants.TableName, CreateColumnDefinitions());
        await DatabricksSchemaManager.InsertAsync(MeasurementsGoldConstants.TableName, CreateRow(new LocalDate(2023, 1, 2)));
    }

    public new async Task DisposeAsync()
    {
        await base.DisposeAsync();
        await DatabricksSchemaManager.DropSchemaAsync();
    }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseSetting($"{DatabricksSqlStatementOptions.DatabricksOptions}:{nameof(DatabricksSqlStatementOptions.WorkspaceUrl)}", IntegrationTestConfiguration.DatabricksSettings.WorkspaceUrl);
        builder.UseSetting($"{DatabricksSqlStatementOptions.DatabricksOptions}:{nameof(DatabricksSqlStatementOptions.WorkspaceToken)}", IntegrationTestConfiguration.DatabricksSettings.WorkspaceAccessToken);
        builder.UseSetting($"{DatabricksSqlStatementOptions.DatabricksOptions}:{nameof(DatabricksSqlStatementOptions.WarehouseId)}", IntegrationTestConfiguration.DatabricksSettings.WarehouseId);
        builder.UseSetting($"{DatabricksSchemaOptions.SectionName}:{nameof(DatabricksSchemaOptions.SchemaName)}", DatabricksSchemaManager.SchemaName);
        builder.UseSetting($"{DatabricksSchemaOptions.SectionName}:{nameof(DatabricksSchemaOptions.CatalogName)}", CatalogName);
        builder.UseSetting($"{AuthenticationOptions.SectionName}:{nameof(AuthenticationOptions.ApplicationIdUri)}", ApplicationIdUri);
        builder.UseSetting($"{AuthenticationOptions.SectionName}:{nameof(AuthenticationOptions.Issuer)}", Issuer);
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
            { MeasurementsGoldConstants.IsCancelledColumnName, ("BOOLEAN", true) },
        };

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
            "false",
        });
    }
}

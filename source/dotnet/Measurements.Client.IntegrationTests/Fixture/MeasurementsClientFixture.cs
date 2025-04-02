using Energinet.DataHub.Core.Databricks.SqlStatementExecution;
using Energinet.DataHub.Core.FunctionApp.TestCommon.Configuration;
using Energinet.DataHub.Core.FunctionApp.TestCommon.Databricks;
using Energinet.DataHub.Measurements.Application.Extensions.Options;
using Energinet.DataHub.Measurements.Client.Extensions;
using Energinet.DataHub.Measurements.Client.Extensions.DependencyInjection;
using Energinet.DataHub.Measurements.Client.Extensions.Options;
using Energinet.DataHub.Measurements.Infrastructure.Persistence;
using Energinet.DataHub.Measurements.WebApi;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using NodaTime;
using Xunit;

namespace Energinet.DataHub.Measurements.Client.IntegrationTests.Fixture;

public sealed class MeasurementsClientFixture : IAsyncLifetime
{
    private const string ApplicationIdUri = "https://management.azure.com";
    private const string Issuer = "https://sts.windows.net/f7619355-6c67-4100-9a78-1847f30742e2/";
    private const string CatalogName = "hive_metastore";
    private const string MeasurementsApiBaseAddress = "http://localhost:5000";

    public MeasurementsClientFixture()
    {
        IntegrationTestConfiguration = new IntegrationTestConfiguration();
        DatabricksSchemaManager = new DatabricksSchemaManager(
            new HttpClientFactory(),
            IntegrationTestConfiguration.DatabricksSettings,
            "mmcore_measurementsapi");
        MeasurementsApiHost = BuildMeasurementsApiHost();
        ServiceProvider = BuildServiceProvider();
    }

    public static string TestMeteringPointId => "1234567890";

    public static LocalDate TestDate => new(2023, 1, 2);

    public ServiceProvider ServiceProvider { get; }

    private IWebHost MeasurementsApiHost { get; }

    private DatabricksSchemaManager DatabricksSchemaManager { get; }

    private IntegrationTestConfiguration IntegrationTestConfiguration { get; }

    public async Task InitializeAsync()
    {
        await DatabricksSchemaManager.CreateSchemaAsync();
        await DatabricksSchemaManager.CreateTableAsync(MeasurementsGoldConstants.TableName, CreateColumnDefinitions());
        await DatabricksSchemaManager.InsertAsync(MeasurementsGoldConstants.TableName, CreateRow());
        await MeasurementsApiHost.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await DatabricksSchemaManager.DropSchemaAsync();
        await MeasurementsApiHost.StopAsync();
    }

    private IWebHost BuildMeasurementsApiHost()
    {
        var host = WebHost.CreateDefaultBuilder()
            .ConfigureAppConfiguration((_, config) =>
            {
                config.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    [$"{DatabricksSqlStatementOptions.DatabricksOptions}:{nameof(DatabricksSqlStatementOptions.WorkspaceUrl)}"] = IntegrationTestConfiguration.DatabricksSettings.WorkspaceUrl,
                    [$"{DatabricksSqlStatementOptions.DatabricksOptions}:{nameof(DatabricksSqlStatementOptions.WorkspaceToken)}"] = IntegrationTestConfiguration.DatabricksSettings.WorkspaceAccessToken,
                    [$"{DatabricksSqlStatementOptions.DatabricksOptions}:{nameof(DatabricksSqlStatementOptions.WarehouseId)}"] = IntegrationTestConfiguration.DatabricksSettings.WarehouseId,
                    [$"{DatabricksSchemaOptions.SectionName}:{nameof(DatabricksSchemaOptions.SchemaName)}"] = DatabricksSchemaManager.SchemaName,
                    [$"{DatabricksSchemaOptions.SectionName}:{nameof(DatabricksSchemaOptions.CatalogName)}"] = CatalogName,
                    [$"{AuthenticationOptions.SectionName}:{nameof(AuthenticationOptions.ApplicationIdUri)}"] = ApplicationIdUri,
                    [$"{AuthenticationOptions.SectionName}:{nameof(AuthenticationOptions.Issuer)}"] = Issuer,
                });
            })
            .UseStartup<Startup>()
            .UseUrls(MeasurementsApiBaseAddress)
            .Build();

        return host;
    }

    private static ServiceProvider BuildServiceProvider()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [$"{MeasurementHttpClientOptions.SectionName}:{nameof(MeasurementHttpClientOptions.BaseAddress)}"] = MeasurementsApiBaseAddress,
                [$"{MeasurementHttpClientOptions.SectionName}:{nameof(MeasurementHttpClientOptions.ApplicationIdUri)}"] = ApplicationIdUri,
            })
            .Build();

        services.AddScoped<IConfiguration>(_ => configuration);
        services.AddMeasurementsClient();

        return services.BuildServiceProvider();
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

    private static IEnumerable<IEnumerable<string>> CreateRow()
    {
        var observationTime = TestDate.ToUtcDateTimeOffset();

        return Enumerable.Range(0, 24).Select(i => new[]
        {
            $"'{TestMeteringPointId}'",
            "'kwh'",
            $"'{observationTime.AddHours(i).ToFormattedString()}'",
            $"{i}.4",
            "'measured'",
            "'2025-03-12T03:40:55Z'",
            "false",
        });
    }
}

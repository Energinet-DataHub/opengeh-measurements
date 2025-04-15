using Asp.Versioning.Conventions;
using Energinet.DataHub.Measurements.WebApi;

var builder = WebApplication.CreateBuilder(args);
var startup = new Startup(builder.Configuration);

startup.ConfigureServices(builder.Services);

var app = builder.Build();

/*// startup.ConfigureGroups(app);
var versionSet = app.NewApiVersionSet()
    .HasApiVersion(1) // Define the API version
    .ReportApiVersions() // Optional: Include supported versions in the response headers
    .Build();

app.MapGroup("v{version:apiVersion}/measurements")
    .HasApiVersion(1)
    .WithApiVersionSet(versionSet)
    .WithTags("v1");

app.MapGroup("/measurements")
    .HasApiVersion(1)
    .WithApiVersionSet(versionSet)
    .WithTags("Default v1")
    .MapControllers();*/

startup.Configure(app);

app.Run();

// Enable testing
namespace Energinet.DataHub.Measurements.WebApi
{
    // ReSharper disable once PartialTypeWithSinglePart
    public partial class Program { }
}

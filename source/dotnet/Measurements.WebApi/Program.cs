using Energinet.DataHub.Measurements.WebApi;

var builder = WebApplication.CreateBuilder(args);
var startup = new Startup(builder.Configuration);

startup.ConfigureServices(builder.Services);

var app = builder.Build();

startup.Configure(app);

app.Run();

// Enable testing
namespace Energinet.DataHub.Measurements.WebApi
{
    // ReSharper disable once PartialTypeWithSinglePart
    public partial class Program { }
}

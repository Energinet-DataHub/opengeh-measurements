using Energinet.DataHub.Core.App.WebApp.Extensions.Builder;
using Energinet.DataHub.Measurements.WebApi;

var app = ApplicationFactory.CreateBuilder(args).Build();

/*
// Configure the HTTP request pipeline.
*/

app.UseRouting();
app.UseSwaggerForWebApp();
app.UseHttpsRedirection();

// Authentication/authorization
app.UseAuthentication();
app.UseAuthorization();

// Health Checks
app.MapLiveHealthChecks();
app.MapReadyHealthChecks();
app.MapStatusHealthChecks();
app.MapControllers();

app.Run();

// Enable testing
namespace Energinet.DataHub.Measurements.WebApi
{
    // ReSharper disable once PartialTypeWithSinglePart
    public partial class Program { }
}

# Documentation

Documentation of the NuGet package bundle `Measurements.Client` which contains the NuGet packages:

- `Measurements.Abstractions`
- `Measurements.Client`

## Measurements.Abstractions

Contains queries and response models used by `Measurements.Client`.

## Measurements.Client

Contains methods to query the `Measurements.WebApi`. See [IMeasurementsClient](https://github.com/Energinet-DataHub/opengeh-measurements/blob/main/source/dotnet/Measurements.Client/IMeasurementsClient.cs) for a description of each method.

### Registering Measurements.Client in Dependency Injection

To use the `Measurements.Client` in your application, you need to register it in your dependency injection container. Follow these steps:

1. **Install the NuGet Package**
   Ensure you have installed the `Measurements.Client` NuGet package in your project:

   ```bash
   dotnet add package Energinet.DataHub.Measurements.Client
   ```

2. **Add the Registration Code**
   In your applications startup configuration (e.g., Startup.cs or Program.cs), register the `Measurements.Client` services. Use the provided extension method `AddMeasurementsClient` to simplify the registration process.

   ```csharp
   using Measurements.Client;

   var builder = WebApplication.CreateBuilder(args);

   // Register Measurements.Client
   builder.Services.AddMeasurementsClient();

   var app = builder.Build();

   // ...existing code...

   app.Run();
   ```

   Notice, the `AddMeasurementsClient` takes an optional `AuthorizationHeaderProvider`, which must implement the `IAuthorizationHeaderProvider` [IAuthorizationHeaderProvider](https://github.com/Energinet-DataHub/geh-core/blob/main/source/App/source/Common/Identity/IAuthorizationHeaderProvider.cs) interface. This is to enable creating a custom authentication header for the temporary B2C authentication model supporting ElOverblik and Energy Track & Trace:

   ```csharp
      services.AddMeasurementsClient(new B2CAuthorizationHeaderProvider());
   ```

3. **Inject and Use the Client**
   Once registered, you can inject the `IMeasurementsClient` interface into your services or controllers and use it to interact with the `Measurements.WebApi`.

   ```csharp
   using Measurements.Client;

   public class YourService
   {
       private readonly IMeasurementsClient _measurementsClient;

       public YourService(IMeasurementsClient measurementsClient)
       {
           _measurementsClient = measurementsClient;
       }

       public async Task GetMeasurementsAsync()
       {
           var measurements = await _measurementsClient.GetByPeriodAsync();
           // Process measurements...
       }
   }
   ```

4. **Configure the application**
   Configure environment varibles corresponding to the options of [MeasurementHttpClientOptions](https://github.com/Energinet-DataHub/opengeh-measurements/blob/main/source/dotnet/Measurements.Client/Extensions/Options/MeasurementHttpClientOptions.cs)

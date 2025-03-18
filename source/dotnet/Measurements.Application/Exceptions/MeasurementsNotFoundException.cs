namespace Energinet.DataHub.Measurements.Application.Exceptions;

public class MeasurementsNotFoundException(string message = "No measurements found for metering point during period")
    : Exception(message);

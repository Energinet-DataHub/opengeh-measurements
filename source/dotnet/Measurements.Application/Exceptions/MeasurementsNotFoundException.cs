namespace Energinet.DataHub.Measurements.Application.Exceptions;

public class MeasurementsNotFoundException(string message = "Not measurements found for metering point during period")
    : Exception(message);

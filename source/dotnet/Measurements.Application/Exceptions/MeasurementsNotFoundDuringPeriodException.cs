namespace Energinet.DataHub.Measurements.Application.Exceptions;

public class MeasurementsNotFoundDuringPeriodException(string message = "No measurements found for metering point during period")
    : Exception(message);

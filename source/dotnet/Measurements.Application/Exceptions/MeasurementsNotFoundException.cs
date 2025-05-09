﻿namespace Energinet.DataHub.Measurements.Application.Exceptions;

public class MeasurementsNotFoundException(string message = "No measurements found for metering point")
    : Exception(message);

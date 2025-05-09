﻿namespace Energinet.DataHub.Measurements.Domain;

public record MeasurementAggregationByDate(
    DateOnly Date,
    decimal Quantity,
    Quality Quality,
    Unit Unit,
    bool MissingValues,
    bool ContainsUpdatedValues);

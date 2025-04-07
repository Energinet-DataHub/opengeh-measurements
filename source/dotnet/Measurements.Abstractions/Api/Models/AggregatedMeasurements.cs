using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

public sealed record MeasurementAggregationDto(DateOnly Date, decimal Quantity, Quality Quality, bool MissingValues);

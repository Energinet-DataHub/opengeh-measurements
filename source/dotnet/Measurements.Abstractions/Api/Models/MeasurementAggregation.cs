using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

public sealed record MeasurementAggregation(LocalDate Date, decimal Quantity, Quality Quality, bool MissingValues);

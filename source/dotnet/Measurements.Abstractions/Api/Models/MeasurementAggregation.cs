using NodaTime;

namespace Energinet.DataHub.Measurements.Abstractions.Api.Models;

public record MeasurementAggregation(LocalDate Date, decimal Quantity, bool MissingValues);

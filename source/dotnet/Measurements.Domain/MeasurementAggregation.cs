using NodaTime;

namespace Energinet.DataHub.Measurements.Domain;

public record MeasurementAggregation(Instant MinObservationTime, Instant MaxObservationTime, decimal Quantity, IEnumerable<Quality> Qualities, IEnumerable<Resolution> Resolutions, long PointCount);

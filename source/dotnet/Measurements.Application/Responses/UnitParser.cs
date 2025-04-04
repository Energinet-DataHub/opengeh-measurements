using Energinet.DataHub.Measurements.Domain;

namespace Energinet.DataHub.Measurements.Application.Responses;

public class UnitParser
{
    public static Unit ParseUnit(string unit)
    {
        return unit.ToLower() switch
        {
            "kwh" => Unit.kWh,
            "kw" => Unit.kW,
            "mw" => Unit.MW,
            "mwh" => Unit.MWh,
            "tonne" => Unit.Tonne,
            "kvarh" => Unit.kVArh,
            "mvar" => Unit.MVAr,
            _ => throw new ArgumentOutOfRangeException(nameof(unit)),
        };
    }
}

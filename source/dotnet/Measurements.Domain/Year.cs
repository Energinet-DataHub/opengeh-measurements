using System.ComponentModel.DataAnnotations;
using NodaTime;

namespace Energinet.DataHub.Measurements.Domain;

public readonly struct Year([Range(-9998, 9999)] int year) : IEquatable<Year>
{
    public int GetYear() => _year;

    private LocalDate StartDate { get; } = new(year, 1, 1);

    private LocalDate EndDate { get; } = new(year, 12, 31);

    public (LocalDate StartDate, LocalDate EndDate) ToDateInterval()
    {
        return (StartDate, EndDate);
    }

    public override int GetHashCode()
    {
        return _year.GetHashCode();
    }

    private readonly int _year = year;

    public bool Equals(Year other)
    {
        return other._year == _year;
    }

    public override bool Equals(object? obj)
    {
        return obj is Year year && Equals(year);
    }

    public static bool operator ==(Year left, Year right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(Year left, Year right)
    {
        return !(left == right);
    }
}

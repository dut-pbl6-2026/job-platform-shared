namespace SharedKernel;

/// <summary>Base value object with equality by components.</summary>
public abstract class ValueObject : IEquatable<ValueObject>
{
    /// <summary>Gets components for equality.</summary>
    protected abstract IEnumerable<object?> GetEqualityComponents();
    /// <summary>Equality check.</summary>
    public override bool Equals(object? obj) => obj is ValueObject other && Equals(other);
    /// <summary>Typed equality.</summary>
    public bool Equals(ValueObject? other) => other is not null && GetType() == other.GetType() && GetEqualityComponents().SequenceEqual(other.GetEqualityComponents());
    /// <summary>Hash code.</summary>
    public override int GetHashCode() => GetEqualityComponents().Aggregate(0, (h, c) => HashCode.Combine(h, c?.GetHashCode() ?? 0));
    /// <summary>Equality operator.</summary>
    public static bool operator ==(ValueObject? a, ValueObject? b) => a is null && b is null || a is not null && a.Equals(b);
    /// <summary>Inequality operator.</summary>
    public static bool operator !=(ValueObject? a, ValueObject? b) => !(a == b);
}

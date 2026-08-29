namespace SharedKernel;

public abstract class ValueObject
{
    protected abstract IEnumerable<object?> GetEqualityComponents();
    public override bool Equals(object? obj)
    {
        if (obj is null || GetType()!=obj.GetType()) return false;
        var other=(ValueObject)obj;
        return GetEqualityComponents().SequenceEqual(other.GetEqualityComponents());
    }
    public override int GetHashCode() => GetEqualityComponents().Aggregate(0, (h,c)=>HashCode.Combine(h,c?.GetHashCode()??0));
}

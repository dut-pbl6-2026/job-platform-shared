namespace SharedKernel;

/// <summary>Base entity with identity and timestamps.</summary>
public abstract class Entity
{
    /// <summary>Primary key.</summary>
    public Guid Id { get; protected set; } = Guid.NewGuid();
    /// <summary>Creation time UTC.</summary>
    public DateTime CreatedAt { get; protected set; } = DateTime.UtcNow;
    /// <summary>Last update time UTC.</summary>
    public DateTime UpdatedAt { get; protected set; } = DateTime.UtcNow;
    /// <summary>Updates UpdatedAt to now.</summary>
    public void Touch() => UpdatedAt = DateTime.UtcNow;
}

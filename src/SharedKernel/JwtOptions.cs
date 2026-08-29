namespace SharedKernel;

/// <summary>JWT options bound from configuration.</summary>
public class JwtOptions
{
    /// <summary>Config section name.</summary>
    public const string SectionName = "Jwt";
    /// <summary>Signing secret (min 32 chars).</summary>
    public string Secret { get; set; } = "";
    /// <summary>Issuer.</summary>
    public string Issuer { get; set; } = "job-platform";
    /// <summary>Audience.</summary>
    public string Audience { get; set; } = "job-platform";
    /// <summary>Expiry minutes (default 60).</summary>
    public int ExpiresMinutes { get; set; } = 60;
}

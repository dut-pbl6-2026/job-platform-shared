namespace SharedKernel;

/// <summary>
/// JWT settings bound from configuration. Tokens are RS256, signed by auth-svc; services validate them
/// with the public keys from auth-svc's JWKS, so this type holds no key material.
/// </summary>
public class JwtOptions
{
    /// <summary>Config section name.</summary>
    public const string SectionName = "Jwt";
    /// <summary>Issuer.</summary>
    public string Issuer { get; set; } = "job-platform";
    /// <summary>Audience.</summary>
    public string Audience { get; set; } = "job-platform";
    /// <summary>Expiry minutes (default 60).</summary>
    public int ExpiresMinutes { get; set; } = 60;
}

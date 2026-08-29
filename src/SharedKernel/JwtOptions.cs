namespace SharedKernel;

public class JwtOptions
{
    public const string SectionName="Jwt";
    public string Secret { get; set; } = "";
    public string Issuer { get; set; } = "job-platform";
    public string Audience { get; set; } = "job-platform";
    public int ExpiresMinutes { get; set; } = 60;
}

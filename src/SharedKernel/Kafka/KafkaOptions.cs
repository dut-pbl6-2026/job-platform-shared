namespace SharedKernel.Kafka;

/// <summary>
/// Shared Kafka connection options (transport level only). Topic and GroupId stay
/// consumer-specific in each service (e.g. search-svc <c>KafkaOptions</c> with
/// <c>Topic = job-events</c>, <c>GroupId = search-svc</c>).
/// Binds <c>KAFKA_BOOTSTRAP_SERVERS</c> then <c>Kafka:BootstrapServers</c>; SASL/TLS for production.
/// Dev: leave SASL empty (plaintext, no auth). Prod: set username + password
/// (auto-selects SaslSsl) or pin an explicit <see cref="SecurityProtocol"/>.
/// </summary>
public class KafkaOptions
{
    /// <summary>Config section name.</summary>
    public const string SectionName = "Kafka";

    /// <summary>Broker list (e.g. from KAFKA_BOOTSTRAP_SERVERS). Empty means Kafka disabled.</summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>SASL username for production brokers. Empty means no SASL (dev).</summary>
    public string SaslUsername { get; set; } = string.Empty;

    /// <summary>SASL password for production brokers. Required when <see cref="SaslUsername"/> is set.</summary>
    public string SaslPassword { get; set; } = string.Empty;

    /// <summary>
    /// Security protocol: <c>Plaintext</c>, <c>SaslPlaintext</c>, <c>SaslSsl</c> or <c>Ssl</c>
    /// (case-insensitive). Empty means auto: <c>SaslSsl</c> when <see cref="SaslUsername"/>
    /// is set, otherwise <c>Plaintext</c>. Explicit <c>SaslPlaintext</c> is allowed for
    /// staging behind a private network only — never for production credentials.
    /// </summary>
    public string SecurityProtocol { get; set; } = string.Empty;

    /// <summary>
    /// SASL mechanism: <c>Plain</c> (default), <c>ScramSha256</c> or <c>ScramSha512</c>
    /// (case-insensitive). Used only when <see cref="SaslUsername"/> is set.
    /// </summary>
    public string SaslMechanism { get; set; } = "Plain";

    /// <summary>
    /// Path to a CA certificate file for <c>Ssl</c>/<c>SaslSsl</c>. Optional; when set,
    /// the file must exist (checked by <see cref="Validate"/>).
    /// </summary>
    public string SslCaLocation { get; set; } = string.Empty;

    /// <summary>Validates security settings. Fail-fast on unsafe or unknown configuration.</summary>
    /// <exception cref="InvalidOperationException">
    /// Username without password, unknown protocol/mechanism, or missing CA file.
    /// </exception>
    public void Validate()
    {
        if (!string.IsNullOrWhiteSpace(SaslUsername) && string.IsNullOrWhiteSpace(SaslPassword))
        {
            throw new InvalidOperationException(
                "Kafka SaslUsername is set but SaslPassword is empty. Refusing to start with half-configured SASL.");
        }

        if (!string.IsNullOrWhiteSpace(SecurityProtocol)
            && !Enum.TryParse<Confluent.Kafka.SecurityProtocol>(SecurityProtocol.Trim(), ignoreCase: true, out _))
        {
            throw new InvalidOperationException(
                $"Unknown Kafka SecurityProtocol '{SecurityProtocol}'. Valid: Plaintext, SaslPlaintext, SaslSsl, Ssl.");
        }

        if (!string.IsNullOrWhiteSpace(SaslMechanism)
            && !Enum.TryParse<Confluent.Kafka.SaslMechanism>(SaslMechanism.Trim(), ignoreCase: true, out _))
        {
            throw new InvalidOperationException(
                $"Unknown Kafka SaslMechanism '{SaslMechanism}'. Valid: Plain, ScramSha256, ScramSha512, Gssapi, OAuthBearer.");
        }

        if (!string.IsNullOrWhiteSpace(SslCaLocation) && !File.Exists(SslCaLocation.Trim()))
        {
            throw new InvalidOperationException(
                $"Kafka SslCaLocation file not found: '{SslCaLocation}'.");
        }
    }
}

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

    /// <summary>Default suffix of the dead-letter topic: <c>&lt;original topic&gt;.dlq</c>.</summary>
    public const string DefaultDeadLetterTopicSuffix = ".dlq";

    /// <summary>
    /// When true (default), a message whose handler retry budget is exhausted is published to
    /// <c>&lt;original topic&gt;</c> + <see cref="DeadLetterTopicSuffix"/> and the partition moves on.
    /// When false, the partition stops (uncommitted) until restart, as before dead-lettering existed.
    /// </summary>
    public bool DeadLetterEnabled { get; set; } = true;

    /// <summary>
    /// Suffix appended to the consumed topic to get the dead-letter topic name. Only letters,
    /// digits, <c>.</c>, <c>_</c> and <c>-</c>; must not be empty (an empty suffix would
    /// make the dead-letter topic the source topic).
    /// </summary>
    public string DeadLetterTopicSuffix { get; set; } = DefaultDeadLetterTopicSuffix;

    /// <summary>Validates security settings. Fail-fast on unsafe or unknown configuration.</summary>
    /// <exception cref="InvalidOperationException">
    /// Username without password, unknown protocol/mechanism, missing CA file, or an invalid
    /// dead-letter topic suffix.
    /// </exception>
    public void Validate()
    {
        if (DeadLetterTopicSuffix is null
            || DeadLetterTopicSuffix.Length == 0
            || !DeadLetterTopicSuffix.All(c => char.IsAsciiLetterOrDigit(c) || c is '.' or '_' or '-'))
        {
            throw new InvalidOperationException(
                $"Invalid Kafka DeadLetterTopicSuffix '{DeadLetterTopicSuffix}'. Use only ASCII letters, digits, '.', '_' or '-', and do not leave it empty.");
        }

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

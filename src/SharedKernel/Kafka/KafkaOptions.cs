namespace SharedKernel.Kafka;

/// <summary>
/// Shared Kafka connection options (transport level only). Topic and GroupId stay
/// consumer-specific in each service (e.g. search-svc <c>KafkaOptions</c> with
/// <c>Topic = job-events</c>, <c>GroupId = search-svc</c>).
/// Binds <c>KAFKA_BOOTSTRAP_SERVERS</c> then <c>Kafka:BootstrapServers</c>; SASL only for production.
/// </summary>
public class KafkaOptions
{
    /// <summary>Config section name.</summary>
    public const string SectionName = "Kafka";

    /// <summary>Broker list (e.g. from KAFKA_BOOTSTRAP_SERVERS). Empty means Kafka disabled.</summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>SASL username for production brokers. Empty means no SASL (dev).</summary>
    public string SaslUsername { get; set; } = string.Empty;

    /// <summary>SASL password for production brokers. Empty means no SASL (dev).</summary>
    public string SaslPassword { get; set; } = string.Empty;
}

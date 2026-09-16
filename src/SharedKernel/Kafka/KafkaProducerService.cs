using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SharedKernel.Events;

namespace SharedKernel.Kafka;

/// <summary>Abstraction over the shared Kafka producer (DI + testability).</summary>
public interface IKafkaProducer : IDisposable
{
    /// <summary>
    /// Produces one envelope. The key is required: producers must pass the entity id
    /// (JobId / ApplicationId) so one entity stays ordered on one partition (plan 2.2.1).
    /// Throws on broker failure; the caller logs a structured ERROR with the entity id
    /// and decides fallback (plan 2.6, service-side circuit-breaker).
    /// </summary>
    /// <typeparam name="T">Payload type.</typeparam>
    /// <param name="topic">Target topic (e.g. job-events).</param>
    /// <param name="key">Partition key (entity id). Must not be empty.</param>
    /// <param name="envelope">Envelope to publish.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Task for the produce operation.</returns>
    Task ProduceAsync<T>(string topic, string key, EventEnvelope<T> envelope, CancellationToken ct = default);
}

/// <summary>
/// Generic Kafka producer wrapper (plan Task 3.2). Idempotent producer
/// (<c>Acks.All</c> + idempotence) for at-least-once delivery; consumers must be
/// idempotent (plan B-6/B-7). Registered per service in PR2/PR4; disposed by DI.
/// </summary>
public sealed class KafkaProducerService : IKafkaProducer
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducerService> _logger;
    private bool _disposed;

    /// <summary>Builds the underlying producer. Fail-fast when the broker is not configured.</summary>
    /// <param name="options">Shared Kafka options (bootstrap servers, SASL).</param>
    /// <param name="logger">Logger.</param>
    /// <exception cref="InvalidOperationException">BootstrapServers is empty.</exception>
    public KafkaProducerService(IOptions<KafkaOptions> options, ILogger<KafkaProducerService> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        var kafka = options.Value ?? throw new InvalidOperationException("KafkaOptions not configured.");
        if (string.IsNullOrWhiteSpace(kafka.BootstrapServers))
        {
            throw new InvalidOperationException("Kafka not configured. Set KAFKA_BOOTSTRAP_SERVERS or Kafka:BootstrapServers.");
        }

        var config = new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers.Trim(),
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageTimeoutMs = 10_000,
        };
        ApplySasl(config, kafka);
        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    /// <inheritdoc />
    public async Task ProduceAsync<T>(string topic, string key, EventEnvelope<T> envelope, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(envelope);

        var value = JsonSerializer.Serialize(envelope, JsonOptions);
        var report = await _producer.ProduceAsync(
            topic.Trim(),
            new Message<string, string> { Key = key, Value = value },
            ct);
        _logger.LogDebug(
            "Kafka produced {EventType} key={Key} to {Offset}.",
            envelope.EventType, key, report.TopicPartitionOffset);
    }

    /// <summary>Flushes and releases the producer.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _producer.Dispose();
    }

    private static void ApplySasl(ClientConfig config, KafkaOptions kafka)
    {
        // Dev default: plaintext, no auth. Prod TODO (SEC-K2/K3): SASL/SCRAM + TLS via env.
        if (string.IsNullOrWhiteSpace(kafka.SaslUsername))
        {
            return;
        }

        config.SecurityProtocol = SecurityProtocol.SaslPlaintext;
        config.SaslMechanism = SaslMechanism.Plain;
        config.SaslUsername = kafka.SaslUsername;
        config.SaslPassword = kafka.SaslPassword;
    }
}

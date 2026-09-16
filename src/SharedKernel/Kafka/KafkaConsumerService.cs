using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SharedKernel.Events;

namespace SharedKernel.Kafka;

/// <summary>
/// Generic Kafka consumer base (plan Task 3.2). Single-threaded consume loop, one message
/// at a time; manual offset commit only after <see cref="HandleMessageAsync"/> succeeds,
/// so crashes redeliver (at-least-once, plan B-6/B-7) and handlers must be idempotent.
/// When no broker is configured the service stays idle (startup graceful degradation,
/// plan 2.3) instead of crashing.
/// </summary>
public abstract class KafkaConsumerService : BackgroundService
{
    private readonly KafkaOptions _options;
    private readonly ILogger _logger;

    /// <summary>Topic to subscribe (e.g. job-events).</summary>
    protected abstract string Topic { get; }

    /// <summary>Consumer group id. Unique per service (SEC-K5).</summary>
    protected abstract string GroupId { get; }

    /// <summary>Creates the consumer base.</summary>
    /// <param name="options">Shared Kafka options (bootstrap servers, SASL).</param>
    /// <param name="logger">Logger (pass the typed logger of the derived class).</param>
    protected KafkaConsumerService(IOptions<KafkaOptions> options, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options.Value ?? throw new InvalidOperationException("KafkaOptions not configured.");
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Handles one consumed message. Throw to skip the commit and redeliver later.
    /// Implementations must own a DI scope per call
    /// (<c>IServiceProvider.CreateScope()</c>, plan 2.4) and stay idempotent.
    /// </summary>
    /// <param name="key">Message key (entity id), may be null for legacy messages.</param>
    /// <param name="value">Raw JSON envelope.</param>
    /// <param name="ct">Shutdown token.</param>
    /// <returns>Task for the handling.</returns>
    protected abstract Task HandleMessageAsync(string? key, string value, CancellationToken ct);

    /// <summary>
    /// Parses an envelope. Returns false for poison messages (SEC-K4): the caller must
    /// log a warning and commit to skip instead of looping forever.
    /// </summary>
    /// <typeparam name="T">Payload type.</typeparam>
    /// <param name="json">Raw message value.</param>
    /// <param name="envelope">Parsed envelope, if valid.</param>
    /// <returns>True when the envelope parsed and carries a payload.</returns>
    protected static bool TryParseEnvelope<T>(string json, out EventEnvelope<T>? envelope)
    {
        try
        {
            envelope = JsonSerializer.Deserialize<EventEnvelope<T>>(json);
            return envelope is not null && envelope.Payload is not null;
        }
        catch (JsonException)
        {
            envelope = null;
            return false;
        }
    }

    /// <summary>Runs the consume loop until shutdown. 1s poll timeout keeps shutdown responsive.</summary>
    /// <param name="stoppingToken">Shutdown token.</param>
    /// <returns>Task for the loop.</returns>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (string.IsNullOrWhiteSpace(_options.BootstrapServers))
        {
            _logger.LogWarning(
                "Kafka not configured (KAFKA_BOOTSTRAP_SERVERS empty). {Consumer} idle on topic {Topic}.",
                GetType().Name, Topic);
            await Task.Delay(Timeout.Infinite, stoppingToken).ContinueWith(
                _ => { }, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default);
            return;
        }

        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers.Trim(),
            GroupId = GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
        };
        ApplySasl(config, _options);

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(Topic);
        _logger.LogInformation(
            "{Consumer} started. Topic={Topic} Group={Group}.",
            GetType().Name, Topic, GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result;
                try
                {
                    result = consumer.Consume(TimeSpan.FromSeconds(1));
                }
                catch (ConsumeException ex)
                {
                    _logger.LogWarning(ex, "Kafka consume error on topic {Topic}.", Topic);
                    continue;
                }

                if (result is null || result.IsPartitionEOF)
                {
                    continue;
                }

                try
                {
                    await HandleMessageAsync(result.Message.Key, result.Message.Value, stoppingToken);
                    consumer.Commit(result);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex, "Kafka handle failed topic={Topic} key={Key} offset={Offset}. Will redeliver.",
                        result.Topic, result.Message.Key, result.Offset);
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Graceful shutdown.
        }
        finally
        {
            consumer.Close();
            _logger.LogInformation("{Consumer} stopped.", GetType().Name);
        }
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

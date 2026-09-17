using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SharedKernel.Events;

namespace SharedKernel.Kafka;

/// <summary>
/// Generic Kafka consumer base (plan Task 3.2). Single-threaded consume loop, one message
/// at a time. When no broker is configured the service stays idle (startup graceful
/// degradation, plan 2.3) instead of crashing. Consume errors back off exponentially
/// (1s..30s) and a fatally-broken client is rebuilt instead of spinning silently.
/// </summary>
/// <remarks>
/// Delivery contract: at-least-once. The offset commits only after
/// <see cref="MessageOutcome.Handled"/> or <see cref="MessageOutcome.Skip"/>.
/// A crash between a side effect and the commit redelivers the same
/// <c>EventEnvelope.EventId</c>, so handlers must dedupe on it (e.g. unique
/// <c>(application_id, event_type, status_snapshot)</c> in notif email_logs,
/// ES document <c>_id</c> = job id in search). Side effects that cannot be made
/// idempotent need a transactional outbox/inbox in the owning service — out of
/// scope for this shared base.
/// </remarks>
public abstract class KafkaConsumerService : BackgroundService
{
    private readonly KafkaOptions _options;
    private readonly ILogger _logger;

    /// <summary>Topic to subscribe (e.g. job-events).</summary>
    protected abstract string Topic { get; }

    /// <summary>Consumer group id. Unique per service (SEC-K5).</summary>
    protected abstract string GroupId { get; }

    /// <summary>Creates the consumer base.</summary>
    /// <param name="options">Shared Kafka options (bootstrap servers, SASL/TLS).</param>
    /// <param name="logger">Logger (pass the typed logger of the derived class).</param>
    protected KafkaConsumerService(IOptions<KafkaOptions> options, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options.Value ?? throw new InvalidOperationException("KafkaOptions not configured.");
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _options.Validate();
    }

    /// <summary>
    /// Handles one consumed message and returns the commit decision.
    /// Implementations must own a DI scope per call
    /// (<c>IServiceProvider.CreateScope()</c>, plan 2.4) and stay idempotent
    /// (see the at-least-once contract above). Typical flow: parse with
    /// <see cref="TryParseEnvelope{T}"/> — false means poison, log a warning and
    /// return <see cref="MessageOutcome.Skip"/>; transient dependency failure means
    /// <see cref="MessageOutcome.Retry"/>. Throwing is treated like
    /// <see cref="MessageOutcome.Retry"/>.
    /// </summary>
    /// <param name="key">Message key (entity id), may be null for legacy messages.</param>
    /// <param name="value">Raw JSON envelope.</param>
    /// <param name="ct">Shutdown token.</param>
    /// <returns>Handled, Skip or Retry.</returns>
    protected abstract Task<MessageOutcome> HandleMessageAsync(string? key, string value, CancellationToken ct);

    /// <summary>
    /// Parses an envelope with the shared <c>KafkaJson</c> policy. Returns false for
    /// poison messages (SEC-K4): the caller should return
    /// <see cref="MessageOutcome.Skip"/> so the offset commits past it instead of
    /// looping forever.
    /// </summary>
    /// <typeparam name="T">Payload type.</typeparam>
    /// <param name="json">Raw message value.</param>
    /// <param name="envelope">Parsed envelope, if valid.</param>
    /// <returns>True when the envelope parsed and carries a payload.</returns>
    protected static bool TryParseEnvelope<T>(string json, out EventEnvelope<T>? envelope)
    {
        try
        {
            envelope = JsonSerializer.Deserialize<EventEnvelope<T>>(json, KafkaJson.Options);
            return envelope is not null && envelope.Payload is not null;
        }
        catch (JsonException)
        {
            envelope = null;
            return false;
        }
    }

    /// <summary>Runs the consume loop until shutdown, rebuilding the client after fatal errors.</summary>
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

        var generations = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunLoopAsync(stoppingToken);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                generations++;
                var delay = RetryDelay(generations);
                _logger.LogError(
                    ex, "{Consumer} fatal error. Recreating consumer in {Delay}s (generation {Generation}).",
                    GetType().Name, delay.TotalSeconds, generations);
                try
                {
                    await Task.Delay(delay, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }
    }

    private async Task RunLoopAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers.Trim(),
            GroupId = GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
        };
        KafkaSecurity.Apply(config, _options);

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(Topic);
        _logger.LogInformation(
            "{Consumer} started. Topic={Topic} Group={Group}.",
            GetType().Name, Topic, GroupId);

        var failures = 0;
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result;
                try
                {
                    result = consumer.Consume(TimeSpan.FromSeconds(1));
                }
                catch (ConsumeException ex) when (ex.Error.IsFatal)
                {
                    throw new InvalidOperationException(
                        $"Kafka fatal consume error on topic {Topic}: {ex.Error.Reason}", ex);
                }
                catch (ConsumeException ex)
                {
                    failures++;
                    _logger.LogWarning(
                        ex, "Kafka consume error on topic {Topic}. Retrying in {Delay}s.",
                        Topic, RetryDelay(failures).TotalSeconds);
                    if (!await DelayAsync(RetryDelay(failures), stoppingToken))
                    {
                        return;
                    }

                    continue;
                }

                if (result is null || result.IsPartitionEOF)
                {
                    continue;
                }

                MessageOutcome outcome;
                try
                {
                    outcome = await HandleMessageAsync(result.Message.Key, result.Message.Value, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    return;
                }
                catch (Exception ex)
                {
                    failures++;
                    _logger.LogError(
                        ex, "Kafka handle failed topic={Topic} key={Key} offset={Offset}. Will redeliver in {Delay}s.",
                        result.Topic, result.Message.Key, result.Offset, RetryDelay(failures).TotalSeconds);
                    if (!await DelayAsync(RetryDelay(failures), stoppingToken))
                    {
                        return;
                    }

                    continue;
                }

                switch (outcome)
                {
                    case MessageOutcome.Skip:
                        Commit(consumer, result);
                        failures = 0;
                        _logger.LogWarning(
                            "Kafka skipped topic={Topic} key={Key} offset={Offset} (poison/unknown, committed).",
                            result.Topic, result.Message.Key, result.Offset);
                        break;
                    case MessageOutcome.Retry:
                        failures++;
                        _logger.LogWarning(
                            "Kafka handler requested retry topic={Topic} key={Key} offset={Offset}. Retrying in {Delay}s.",
                            result.Topic, result.Message.Key, result.Offset, RetryDelay(failures).TotalSeconds);
                        if (!await DelayAsync(RetryDelay(failures), stoppingToken))
                        {
                            return;
                        }

                        break;
                    default:
                        Commit(consumer, result);
                        failures = 0;
                        break;
                }
            }
        }
        finally
        {
            try
            {
                consumer.Close();
            }
            catch (KafkaException ex)
            {
                _logger.LogDebug(ex, "{Consumer} error during Close.", GetType().Name);
            }

            _logger.LogInformation("{Consumer} stopped.", GetType().Name);
        }
    }

    private void Commit(IConsumer<string, string> consumer, ConsumeResult<string, string> result)
    {
        try
        {
            consumer.Commit(result);
        }
        catch (KafkaException ex)
        {
            // The message was handled; a failed commit only risks redelivery,
            // which idempotent handlers tolerate (at-least-once contract above).
            _logger.LogWarning(
                ex, "Kafka commit failed topic={Topic} offset={Offset}. May redeliver.",
                result.Topic, result.Offset);
        }
    }

    private static async Task<bool> DelayAsync(TimeSpan delay, CancellationToken ct)
    {
        try
        {
            await Task.Delay(delay, ct);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    // Exponential backoff: 1s, 2s, 4s, 8s, 16s, then capped at 30s.
    internal static TimeSpan RetryDelay(int failures)
    {
        var shift = Math.Clamp(failures - 1, 0, 5);
        return TimeSpan.FromSeconds(Math.Min(1 << shift, 30));
    }
}

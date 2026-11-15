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
/// A <see cref="MessageOutcome.Retry"/> (or a thrown exception) seeks back to the failed
/// offset and redelivers it after backoff. When the retry budget
/// (5 handler invocations for one message, about 15s of backoff in total) is exhausted the failed
/// message is published to the dead-letter topic <c>&lt;topic&gt;</c> + <see cref="KafkaOptions.DeadLetterTopicSuffix"/>
/// (default <c>.dlq</c>, see README "Dead-letter topic"). Only after that publish is acknowledged is the
/// offset committed and the partition continues. If dead-lettering is disabled, or the dead-letter
/// publish fails, the partition is paused and no offset at or past the failed message is committed
/// until the service restarts; the failure is logged at Error with topic, partition and offset.
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
    private readonly Func<ConsumerConfig, IKafkaConsumerClient> _clientFactory;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly Func<ProducerConfig, IKafkaRawProducer> _deadLetterProducerFactory;
    private readonly TimeProvider _timeProvider;

    /// <summary>Topic to subscribe (e.g. job-events).</summary>
    protected abstract string Topic { get; }

    /// <summary>Consumer group id. Unique per service (SEC-K5).</summary>
    protected abstract string GroupId { get; }

    /// <summary>Creates the consumer base.</summary>
    /// <param name="options">Shared Kafka options (bootstrap servers, SASL/TLS).</param>
    /// <param name="logger">Logger (pass the typed logger of the derived class).</param>
    protected KafkaConsumerService(IOptions<KafkaOptions> options, ILogger logger)
        : this(
            options,
            logger,
            KafkaConsumerClient.Create,
            (delay, ct) => Task.Delay(delay, ct),
            KafkaRawProducer.Create,
            TimeProvider.System)
    {
    }

    /// <summary>Creates the consumer base with injectable client, delay, dead-letter producer and clock (tests).</summary>
    /// <param name="options">Shared Kafka options.</param>
    /// <param name="logger">Logger.</param>
    /// <param name="clientFactory">Builds the Kafka client from the consumer config.</param>
    /// <param name="delay">Waits for a backoff delay; returns when elapsed or cancelled.</param>
    /// <param name="deadLetterProducerFactory">Builds the producer for dead-letter records from the shared producer config.</param>
    /// <param name="timeProvider">Clock for the dead-letter failed-at header.</param>
    internal KafkaConsumerService(
        IOptions<KafkaOptions> options,
        ILogger logger,
        Func<ConsumerConfig, IKafkaConsumerClient> clientFactory,
        Func<TimeSpan, CancellationToken, Task> delay,
        Func<ProducerConfig, IKafkaRawProducer> deadLetterProducerFactory,
        TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options.Value ?? throw new InvalidOperationException("KafkaOptions not configured.");
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _clientFactory = clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
        _delay = delay ?? throw new ArgumentNullException(nameof(delay));
        _deadLetterProducerFactory = deadLetterProducerFactory ?? throw new ArgumentNullException(nameof(deadLetterProducerFactory));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
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
    /// poison messages (SEC-K4), including null or empty values (Kafka tombstones): the
    /// caller should return <see cref="MessageOutcome.Skip"/> so the offset commits past
    /// it instead of looping forever.
    /// </summary>
    /// <typeparam name="T">Payload type.</typeparam>
    /// <param name="json">Raw message value.</param>
    /// <param name="envelope">Parsed envelope, if valid.</param>
    /// <returns>True when the envelope parsed and carries a payload.</returns>
    protected static bool TryParseEnvelope<T>(string json, out EventEnvelope<T>? envelope)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            envelope = null;
            return false;
        }

        try
        {
            envelope = JsonSerializer.Deserialize<EventEnvelope<T>>(json, KafkaJson.Options);
            if (envelope is null || envelope.Payload is null)
            {
                envelope = null;
                return false;
            }

            return true;
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

        // Fail fast: an invalid dead-letter name would otherwise surface only after a message fails.
        var deadLetterTopic = _options.DeadLetterEnabled
            ? DeadLetter.TopicName(Topic, _options.DeadLetterTopicSuffix)
            : null;

        var generations = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunLoopAsync(deadLetterTopic, stoppingToken);
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
                if (!await DelayAsync(delay, stoppingToken))
                {
                    return;
                }
            }
        }
    }

    private async Task RunLoopAsync(string? deadLetterTopic, CancellationToken stoppingToken)
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

        using var consumer = _clientFactory(config);
        // Null when dead-lettering is disabled: exhausted messages then stop the partition.
        using var deadLetterProducer = deadLetterTopic is null
            ? null
            : _deadLetterProducerFactory(KafkaProducerService.CreateConfig(_options));
        consumer.Subscribe(Topic);
        _logger.LogInformation(
            "{Consumer} started. Topic={Topic} Group={Group} DeadLetterTopic={DeadLetterTopic}.",
            GetType().Name, Topic, GroupId, deadLetterTopic ?? "(disabled)");

        var consumeFailures = 0;
        // Consecutive handler failures for the offset currently being retried, per partition.
        // Kept in this consumer's memory only: a rebalance restarts the budget, which is safe
        // because the offset is committed only after handling or dead-lettering.
        var attempts = new Dictionary<TopicPartition, (Offset Offset, int Count)>();
        // Partitions stopped after an exhausted retry budget. Nothing from them is handled or committed.
        var stopped = new HashSet<TopicPartition>();
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
                    consumeFailures++;
                    _logger.LogWarning(
                        ex, "Kafka consume error on topic {Topic}. Retrying in {Delay}s.",
                        Topic, RetryDelay(consumeFailures).TotalSeconds);
                    if (!await DelayAsync(RetryDelay(consumeFailures), stoppingToken))
                    {
                        return;
                    }

                    continue;
                }

                if (result is null || result.IsPartitionEOF)
                {
                    continue;
                }

                if (stopped.Contains(result.TopicPartition))
                {
                    // Already buffered before the pause took effect. Dropping it keeps the
                    // failed offset uncommitted and nothing past it committed.
                    continue;
                }

                MessageOutcome outcome;
                var exceptionType = DeadLetterHeaders.RetryRequested;
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
                    _logger.LogError(
                        ex, "Kafka handle threw topic={Topic} partition={Partition} key={Key} offset={Offset}. Treating as Retry.",
                        result.Topic, result.Partition.Value, result.Message.Key, result.Offset.Value);
                    outcome = MessageOutcome.Retry;
                    exceptionType = ex.GetType().FullName ?? ex.GetType().Name;
                }

                switch (outcome)
                {
                    case MessageOutcome.Skip:
                        Commit(consumer, result);
                        consumeFailures = 0;
                        attempts.Remove(result.TopicPartition);
                        _logger.LogWarning(
                            "Kafka skipped topic={Topic} key={Key} offset={Offset} (poison/unknown, committed).",
                            result.Topic, result.Message.Key, result.Offset.Value);
                        break;

                    case MessageOutcome.Retry:
                        var tp = result.TopicPartition;
                        var attempt = attempts.TryGetValue(tp, out var previous) && previous.Offset == result.Offset
                            ? previous.Count + 1
                            : 1;
                        attempts[tp] = (result.Offset, attempt);

                        var decision = RetryPolicy.Decide(attempt);
                        if (decision.Retry)
                        {
                            _logger.LogWarning(
                                "Kafka handler requested retry topic={Topic} partition={Partition} key={Key} offset={Offset} attempt={Attempt}. Seeking back and retrying in {Delay}s.",
                                result.Topic, result.Partition.Value, result.Message.Key, result.Offset.Value,
                                attempt, decision.Delay.TotalSeconds);
                            consumer.Seek(result.TopicPartitionOffset);
                            if (!await DelayAsync(decision.Delay, stoppingToken))
                            {
                                return;
                            }

                            break;
                        }

                        // Budget exhausted. Dead-letter the message; commit past it only after the
                        // dead-letter publish is acknowledged. Otherwise stop the partition uncommitted.
                        // This is the last failed invocation, so its exception is the one recorded.
                        var exhaustedType = exceptionType;
                        attempts.Remove(tp);
                        if (deadLetterProducer is not null && deadLetterTopic is not null)
                        {
                            try
                            {
                                var headers = DeadLetter.BuildHeaders(result, attempt, exhaustedType, _timeProvider.GetUtcNow());
                                await deadLetterProducer.ProduceAsync(
                                    deadLetterTopic, result.Message.Key, result.Message.Value, headers, stoppingToken);
                            }
                            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                            {
                                // Shutdown mid-publish: nothing committed, the message is redelivered.
                                return;
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(
                                    ex, "Kafka dead-letter publish failed topic={Topic} partition={Partition} offset={Offset} key={Key} deadLetterTopic={DeadLetterTopic}. Offset not committed; partition paused until restart.",
                                    result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Key, deadLetterTopic);
                                StopPartition(consumer, stopped, tp);
                                break;
                            }

                            Commit(consumer, result);
                            consumeFailures = 0;
                            _logger.LogError(
                                "Kafka message dead-lettered after {Attempts} attempts topic={Topic} partition={Partition} offset={Offset} key={Key} deadLetterTopic={DeadLetterTopic} exceptionType={ExceptionType}. Committed; partition continues.",
                                attempt, result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Key, deadLetterTopic, exhaustedType);
                            break;
                        }

                        StopPartition(consumer, stopped, tp);
                        _logger.LogError(
                            "Kafka partition stopped after {Attempts} attempts topic={Topic} partition={Partition} offset={Offset} key={Key}. Dead-lettering disabled; offset not committed; partition paused until restart.",
                            attempt, result.Topic, result.Partition.Value, result.Offset.Value, result.Message.Key);
                        break;

                    default:
                        Commit(consumer, result);
                        consumeFailures = 0;
                        attempts.Remove(result.TopicPartition);
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

    private void Commit(IKafkaConsumerClient consumer, ConsumeResult<string, string> result)
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
                result.Topic, result.Offset.Value);
        }
    }

    private static void StopPartition(IKafkaConsumerClient consumer, HashSet<TopicPartition> stopped, TopicPartition partition)
    {
        stopped.Add(partition);
        consumer.Pause(partition);
    }

    private async Task<bool> DelayAsync(TimeSpan delay, CancellationToken ct)
    {
        try
        {
            await _delay(delay, ct);
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

/// <summary>Retry-or-stop decision for one message whose handler asked for a retry or threw.</summary>
/// <param name="Retry">True to seek back and redeliver; false to stop the partition.</param>
/// <param name="Delay">Backoff before redelivery (zero when stopping).</param>
internal readonly record struct RetryDecision(bool Retry, TimeSpan Delay);

/// <summary>Pure retry budget logic for the consume loop.</summary>
internal static class RetryPolicy
{
    /// <summary>Handler invocations allowed for one message (first try included) before the partition is stopped.</summary>
    internal const int MaxAttempts = 5;

    /// <summary>
    /// Decides what to do after the <paramref name="attempt"/>-th failed handler invocation
    /// of one message. Attempts 1..MaxAttempts-1 retry after <see cref="KafkaConsumerService.RetryDelay"/>;
    /// the last failed attempt stops the partition.
    /// </summary>
    /// <param name="attempt">1-based count of failed invocations for this offset.</param>
    /// <returns>Retry decision and backoff.</returns>
    internal static RetryDecision Decide(int attempt)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);
        return attempt < MaxAttempts
            ? new RetryDecision(true, KafkaConsumerService.RetryDelay(attempt))
            : new RetryDecision(false, TimeSpan.Zero);
    }
}

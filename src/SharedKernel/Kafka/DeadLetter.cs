using System.Globalization;
using System.Text;
using Confluent.Kafka;

namespace SharedKernel.Kafka;

/// <summary>
/// Header names of the dead-letter envelope. Every record on a dead-letter topic carries the
/// original key, value and headers of the failed message plus these metadata headers.
/// Values are UTF-8 strings. Public because downstream DLQ readers depend on these names.
/// </summary>
public static class DeadLetterHeaders
{
    /// <summary>Topic the failed message was consumed from.</summary>
    public const string OriginalTopic = "dlq-original-topic";

    /// <summary>Partition the failed message was consumed from (decimal).</summary>
    public const string OriginalPartition = "dlq-original-partition";

    /// <summary>Offset of the failed message in its partition (decimal).</summary>
    public const string OriginalOffset = "dlq-original-offset";

    /// <summary>
    /// Full type name of the last exception thrown by the handler for this message, or
    /// <see cref="RetryRequested"/> when the handler only returned <see cref="MessageOutcome.Retry"/>.
    /// The exception message is never included (it can carry personal data).
    /// </summary>
    public const string ExceptionType = "dlq-exception-type";

    /// <summary>Number of handler invocations that failed for this message in this consumer (decimal).</summary>
    public const string AttemptCount = "dlq-attempt-count";

    /// <summary>UTC time the message was dead-lettered, ISO 8601 round-trip ("O") format.</summary>
    public const string FailedAtUtc = "dlq-failed-at-utc";

    /// <summary>Value of <see cref="ExceptionType"/> when the handler returned Retry without throwing.</summary>
    public const string RetryRequested = "MessageOutcome.Retry";

    internal static readonly string[] All =
    [
        OriginalTopic, OriginalPartition, OriginalOffset, ExceptionType, AttemptCount, FailedAtUtc,
    ];
}

/// <summary>Builds dead-letter records and validates dead-letter topic names.</summary>
internal static class DeadLetter
{
    /// <summary>Kafka topic name length limit.</summary>
    internal const int MaxTopicNameLength = 249;

    /// <summary>Builds the dead-letter headers: original headers (minus stale metadata) plus the metadata set.</summary>
    /// <param name="result">Failed message as consumed.</param>
    /// <param name="attempts">Failed handler invocations for this message.</param>
    /// <param name="exceptionType">Full type name of the last exception, or <see cref="DeadLetterHeaders.RetryRequested"/>.</param>
    /// <param name="failedAtUtc">Time of dead-lettering.</param>
    /// <returns>Headers for the dead-letter record.</returns>
    internal static Headers BuildHeaders(
        ConsumeResult<string, string> result,
        int attempts,
        string exceptionType,
        DateTimeOffset failedAtUtc)
    {
        var headers = new Headers();

        // Original headers are kept, but a replayed record must not end up with two copies
        // of a metadata key, so any stale metadata from an earlier dead-letter hop is dropped.
        if (result.Message.Headers is { } original)
        {
            foreach (var header in original)
            {
                if (!Array.Exists(DeadLetterHeaders.All, name => string.Equals(name, header.Key, StringComparison.Ordinal)))
                {
                    headers.Add(header.Key, header.GetValueBytes());
                }
            }
        }

        headers.Add(DeadLetterHeaders.OriginalTopic, Utf8(result.Topic));
        headers.Add(DeadLetterHeaders.OriginalPartition, Utf8(result.Partition.Value.ToString(CultureInfo.InvariantCulture)));
        headers.Add(DeadLetterHeaders.OriginalOffset, Utf8(result.Offset.Value.ToString(CultureInfo.InvariantCulture)));
        headers.Add(DeadLetterHeaders.ExceptionType, Utf8(exceptionType));
        headers.Add(DeadLetterHeaders.AttemptCount, Utf8(attempts.ToString(CultureInfo.InvariantCulture)));
        headers.Add(DeadLetterHeaders.FailedAtUtc, Utf8(failedAtUtc.UtcDateTime.ToString("O", CultureInfo.InvariantCulture)));
        return headers;
    }

    /// <summary>Checks that <c>sourceTopic + suffix</c> is a legal topic name, different from the source.</summary>
    /// <param name="sourceTopic">Topic the consumer subscribes to.</param>
    /// <param name="suffix">Dead-letter suffix from options.</param>
    /// <returns>The dead-letter topic name.</returns>
    /// <exception cref="InvalidOperationException">The combined name is too long or empty.</exception>
    internal static string TopicName(string sourceTopic, string suffix)
    {
        var name = sourceTopic + suffix;
        if (string.IsNullOrWhiteSpace(sourceTopic) || suffix.Length == 0 || name.Length > MaxTopicNameLength)
        {
            throw new InvalidOperationException(
                $"Dead-letter topic name '{name}' is invalid (source '{sourceTopic}', suffix '{suffix}', max {MaxTopicNameLength} characters).");
        }

        return name;
    }

    private static byte[] Utf8(string value) => Encoding.UTF8.GetBytes(value);
}

/// <summary>
/// Raw producer used only for dead-letter records. Unlike <see cref="IKafkaProducer"/> it passes
/// the original key (may be null for legacy messages), the original value (may be null for
/// tombstones) and the headers through unchanged.
/// </summary>
internal interface IKafkaRawProducer : IDisposable
{
    /// <summary>Produces one record and waits for the broker acknowledgement.</summary>
    /// <param name="topic">Target topic.</param>
    /// <param name="key">Original key, or null.</param>
    /// <param name="value">Original value, or null.</param>
    /// <param name="headers">Headers to send.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Task for the produce operation; throws when the broker does not acknowledge.</returns>
    Task ProduceAsync(string topic, string? key, string? value, Headers headers, CancellationToken ct);
}

/// <summary>Production <see cref="IKafkaRawProducer"/> backed by <see cref="IProducer{TKey,TValue}"/>.</summary>
internal sealed class KafkaRawProducer : IKafkaRawProducer
{
    private readonly IProducer<string, string> _producer;

    private KafkaRawProducer(IProducer<string, string> producer)
    {
        _producer = producer;
    }

    /// <summary>Builds a real producer from the given config.</summary>
    /// <param name="config">Producer config (see <see cref="KafkaProducerService.CreateConfig"/>).</param>
    /// <returns>Raw producer wrapping a new Kafka producer.</returns>
    internal static IKafkaRawProducer Create(ProducerConfig config)
        => new KafkaRawProducer(new ProducerBuilder<string, string>(config).Build());

    /// <inheritdoc />
    public async Task ProduceAsync(string topic, string? key, string? value, Headers headers, CancellationToken ct)
    {
        await _producer.ProduceAsync(
            topic,
            new Message<string, string> { Key = key!, Value = value!, Headers = headers },
            ct);
    }

    /// <inheritdoc />
    public void Dispose() => _producer.Dispose();
}

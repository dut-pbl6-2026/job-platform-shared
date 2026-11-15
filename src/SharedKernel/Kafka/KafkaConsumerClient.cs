using Confluent.Kafka;

namespace SharedKernel.Kafka;

/// <summary>
/// The subset of the Kafka consumer the base consume loop uses. Exists so the loop's
/// seek/commit/pause decisions can be exercised without a broker (tests use a fake).
/// </summary>
internal interface IKafkaConsumerClient : IDisposable
{
    /// <summary>Subscribes to a topic.</summary>
    /// <param name="topic">Topic name.</param>
    void Subscribe(string topic);

    /// <summary>Waits up to <paramref name="timeout"/> for the next message.</summary>
    /// <param name="timeout">Maximum wait.</param>
    /// <returns>The next message, or null when none arrived in time.</returns>
    ConsumeResult<string, string>? Consume(TimeSpan timeout);

    /// <summary>Moves the fetch position of one partition (used to redeliver a failed message).</summary>
    /// <param name="position">Partition and target offset.</param>
    void Seek(TopicPartitionOffset position);

    /// <summary>Stops fetching from one partition.</summary>
    /// <param name="partition">Partition to pause.</param>
    void Pause(TopicPartition partition);

    /// <summary>Commits the offset after <paramref name="result"/>.</summary>
    /// <param name="result">Successfully handled message.</param>
    void Commit(ConsumeResult<string, string> result);

    /// <summary>Leaves the group and stops fetching.</summary>
    void Close();
}

/// <summary>Production <see cref="IKafkaConsumerClient"/> backed by <see cref="IConsumer{TKey,TValue}"/>.</summary>
internal sealed class KafkaConsumerClient : IKafkaConsumerClient
{
    private readonly IConsumer<string, string> _consumer;

    private KafkaConsumerClient(IConsumer<string, string> consumer)
    {
        _consumer = consumer;
    }

    /// <summary>Builds a real consumer from the given config.</summary>
    /// <param name="config">Fully populated consumer config.</param>
    /// <returns>Client wrapping a new Kafka consumer.</returns>
    internal static IKafkaConsumerClient Create(ConsumerConfig config)
        => new KafkaConsumerClient(new ConsumerBuilder<string, string>(config).Build());

    /// <inheritdoc />
    public void Subscribe(string topic) => _consumer.Subscribe(topic);

    /// <inheritdoc />
    public ConsumeResult<string, string>? Consume(TimeSpan timeout) => _consumer.Consume(timeout);

    /// <inheritdoc />
    public void Seek(TopicPartitionOffset position) => _consumer.Seek(position);

    /// <inheritdoc />
    public void Pause(TopicPartition partition) => _consumer.Pause([partition]);

    /// <inheritdoc />
    public void Commit(ConsumeResult<string, string> result) => _consumer.Commit(result);

    /// <inheritdoc />
    public void Close() => _consumer.Close();

    /// <inheritdoc />
    public void Dispose() => _consumer.Dispose();
}

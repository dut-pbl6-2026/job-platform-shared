using Confluent.Kafka;
using Microsoft.Extensions.Options;
using SharedKernel.Events;
using SharedKernel.Kafka;

namespace SharedKernel.Tests;

/// <summary>
/// Integration layer: drives the real consume loop of <see cref="KafkaConsumerService"/>
/// against an in-memory client that models partition positions, seek, pause and commit,
/// and an in-memory dead-letter producer. Delays are recorded instead of slept so the suite stays fast.
/// </summary>
public class ConsumerLoopTests
{
    private const string Topic = "job-events";
    private const string DeadLetterTopic = "job-events.dlq";

    [Fact]
    public async Task AllHandled_CommitsEveryOffsetInOrder_WithoutSeek()
    {
        var client = new FakeConsumerClient(Msg(0, 0, "m0"), Msg(0, 1, "m1"), Msg(0, 2, "m2"));
        using var cts = StopWhenIdle(client);
        var consumer = new TestConsumer(client, [], _ => MessageOutcome.Handled);

        await consumer.Run(cts.Token);

        Assert.Equal(new[] { "m0", "m1", "m2" }, consumer.Handled);
        Assert.Equal(new[] { (0, 0L), (0, 1L), (0, 2L) }, client.Commits);
        Assert.Empty(client.Seeks);
        Assert.Empty(client.Pauses);
    }

    [Fact]
    public async Task RetryThenSuccess_SeeksBackAndRedeliversFailedOffset()
    {
        // Regression for the lost-message bug: offset 1 fails once, offset 2 must not
        // commit past it, and offset 1 must be delivered again before offset 2.
        var client = new FakeConsumerClient(Msg(0, 0, "m0"), Msg(0, 1, "m1"), Msg(0, 2, "m2"));
        using var cts = StopWhenIdle(client);
        var delays = new List<TimeSpan>();
        var failedOnce = false;
        var consumer = new TestConsumer(client, delays, value =>
        {
            if (value == "m1" && !failedOnce)
            {
                failedOnce = true;
                return MessageOutcome.Retry;
            }

            return MessageOutcome.Handled;
        });

        await consumer.Run(cts.Token);

        Assert.Equal(new[] { "m0", "m1", "m1", "m2" }, consumer.Handled);
        Assert.Equal(new[] { (0, 1L) }, client.Seeks);
        Assert.Equal(new[] { (0, 0L), (0, 1L), (0, 2L) }, client.Commits);
        Assert.Equal(new[] { TimeSpan.FromSeconds(1) }, delays);
        Assert.Empty(client.Pauses);
    }

    [Fact]
    public async Task HandlerThrowsOnce_IsTreatedAsRetry_SeeksBack()
    {
        var client = new FakeConsumerClient(Msg(0, 0, "m0"), Msg(0, 1, "m1"), Msg(0, 2, "m2"));
        using var cts = StopWhenIdle(client);
        var threw = false;
        var consumer = new TestConsumer(client, [], value =>
        {
            if (value == "m1" && !threw)
            {
                threw = true;
                throw new InvalidOperationException("db down");
            }

            return MessageOutcome.Handled;
        });

        await consumer.Run(cts.Token);

        Assert.Equal(new[] { "m0", "m1", "m1", "m2" }, consumer.Handled);
        Assert.Equal(new[] { (0, 1L) }, client.Seeks);
        Assert.Equal(new[] { (0, 0L), (0, 1L), (0, 2L) }, client.Commits);
        Assert.Empty(client.Pauses);
    }

    [Fact]
    public async Task RetryBudgetExhausted_DeadLetterDisabled_StopsPartition_AndNeverCommitsPastFailedOffset()
    {
        var client = new FakeConsumerClient(
            Msg(0, 0, "m0"), Msg(0, 1, "m1"), Msg(0, 2, "m2"), Msg(0, 3, "m3"));
        using var cts = StopWhenIdle(client);
        var delays = new List<TimeSpan>();
        var producer = new FakeRawProducer(client.Log);
        var consumer = new TestConsumer(client, delays, value =>
            value == "m1" ? MessageOutcome.Retry : MessageOutcome.Handled, producer, deadLetterEnabled: false);

        await consumer.Run(cts.Token);

        // Five handler invocations for m1 (MaxAttempts), then the partition stops.
        Assert.Equal(new[] { "m0", "m1", "m1", "m1", "m1", "m1" }, consumer.Handled);
        Assert.Equal(new[] { (0, 1L), (0, 1L), (0, 1L), (0, 1L) }, client.Seeks);
        Assert.Equal(new[] { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4), TimeSpan.FromSeconds(8) }, delays);
        Assert.Equal(new[] { (0, 0L) }, client.Commits);
        Assert.Equal(new[] { 0 }, client.Pauses);
        Assert.Empty(producer.Published);
    }

    [Fact]
    public async Task RetryBudgetExhausted_DeadLetterDisabled_OnOnePartition_OtherPartitionKeepsCommitting()
    {
        var client = new FakeConsumerClient(
            Msg(0, 0, "p0-m0"), Msg(1, 0, "p1-m0"), Msg(1, 1, "p1-m1"));
        using var cts = StopWhenIdle(client);
        var consumer = new TestConsumer(client, [], value =>
            value == "p0-m0" ? MessageOutcome.Retry : MessageOutcome.Handled, deadLetterEnabled: false);

        await consumer.Run(cts.Token);

        Assert.Equal(new[] { (1, 0L), (1, 1L) }, client.Commits);
        Assert.Equal(new[] { 0 }, client.Pauses);
        Assert.Equal(5, consumer.Handled.Count(v => v == "p0-m0"));
    }

    [Fact]
    public async Task RetryBudget_IsCountedPerPartition_NotShared()
    {
        // Failures on two partitions interleave. Each partition fails four times, which is
        // under the budget of five, so neither may be stopped.
        var client = new FakeConsumerClient(
            Msg(0, 0, "p0-m0"), Msg(0, 1, "p0-m1"),
            Msg(1, 0, "p1-m0"), Msg(1, 1, "p1-m1"));
        using var cts = StopWhenIdle(client);
        var failures = new Dictionary<string, int>();
        var consumer = new TestConsumer(client, [], value =>
        {
            if (value is "p0-m1" or "p1-m1")
            {
                failures[value] = failures.GetValueOrDefault(value) + 1;
                return failures[value] <= 4 ? MessageOutcome.Retry : MessageOutcome.Handled;
            }

            return MessageOutcome.Handled;
        });

        await consumer.Run(cts.Token);

        Assert.Empty(client.Pauses);
        Assert.Equal(new[] { (0, 0L), (0, 1L), (1, 0L), (1, 1L) }, client.Commits.OrderBy(c => c.Partition).ThenBy(c => c.Offset));
    }

    [Fact]
    public async Task Skip_CommitsPastPoisonMessage_WithoutSeek()
    {
        var client = new FakeConsumerClient(Msg(0, 0, "poison"), Msg(0, 1, "m1"));
        using var cts = StopWhenIdle(client);
        var consumer = new TestConsumer(client, [], value =>
            value == "poison" ? MessageOutcome.Skip : MessageOutcome.Handled);

        await consumer.Run(cts.Token);

        Assert.Equal(new[] { (0, 0L), (0, 1L) }, client.Commits);
        Assert.Empty(client.Seeks);
        Assert.Empty(client.Pauses);
    }

    [Fact]
    public async Task NullTombstoneValue_ParsedAsPoison_IsSkippedAndCommitted()
    {
        // A null Kafka value must not throw inside the handler (that would be a Retry and
        // would stop the partition forever). TryParseEnvelope maps it to Skip.
        var client = new FakeConsumerClient(Msg(0, 0, null), Msg(0, 1, "m1"));
        using var cts = StopWhenIdle(client);
        var consumer = new TestConsumer(client, [], value =>
            TestConsumer.TryParse<JobCreatedEvent>(value, out _) ? MessageOutcome.Handled : MessageOutcome.Skip);

        await consumer.Run(cts.Token);

        Assert.Equal(new[] { (0, 0L), (0, 1L) }, client.Commits);
        Assert.Empty(client.Seeks);
        Assert.Empty(client.Pauses);
    }

    [Fact]
    public async Task RetryBudgetExhausted_IsDeadLettered_ThenCommitsAndPartitionContinues()
    {
        // Poison message m1 keeps failing. It must reach the dead-letter topic before its offset
        // is committed, and m2 and m3 must still be handled on the same partition.
        var client = new FakeConsumerClient(
            Msg(0, 0, "m0"), Msg(0, 1, "m1"), Msg(0, 2, "m2"), Msg(0, 3, "m3"));
        using var cts = StopWhenIdle(client);
        var producer = new FakeRawProducer(client.Log);
        var consumer = new TestConsumer(client, [], value =>
            value == "m1" ? MessageOutcome.Retry : MessageOutcome.Handled, producer);

        await consumer.Run(cts.Token);

        Assert.Equal(new[] { "m0", "m1", "m1", "m1", "m1", "m1", "m2", "m3" }, consumer.Handled);
        Assert.Equal(new[] { (0, 1L), (0, 1L), (0, 1L), (0, 1L) }, client.Seeks);
        Assert.Equal(new[] { (0, 0L), (0, 1L), (0, 2L), (0, 3L) }, client.Commits);
        Assert.Empty(client.Pauses);

        var published = Assert.Single(producer.Published);
        Assert.Equal(DeadLetterTopic, published.Topic);
        Assert.Equal("key", published.Key);
        Assert.Equal("m1", published.Value);
        Assert.Single(client.Log.FindAll(entry => entry == "dlq:m1"));
        Assert.True(client.Log.IndexOf("dlq:m1") < client.Log.IndexOf("commit:0:1"),
            "The dead-letter publish must complete before the offset is committed.");
    }

    [Fact]
    public async Task DeadLetterPublishFails_PausesPartition_AndNeverCommitsFailedOffset()
    {
        var client = new FakeConsumerClient(
            Msg(0, 0, "m0"), Msg(0, 1, "m1"), Msg(0, 2, "m2"));
        using var cts = StopWhenIdle(client);
        var producer = new FakeRawProducer(client.Log) { Failure = new InvalidOperationException("broker unavailable") };
        var consumer = new TestConsumer(client, [], value =>
            value == "m1" ? MessageOutcome.Retry : MessageOutcome.Handled, producer);

        await consumer.Run(cts.Token);

        Assert.Equal(new[] { "m0", "m1", "m1", "m1", "m1", "m1" }, consumer.Handled);
        Assert.Equal(new[] { (0, 0L) }, client.Commits);
        Assert.Equal(new[] { 0 }, client.Pauses);
        Assert.Single(producer.Published);
        Assert.DoesNotContain("commit:0:1", client.Log);
    }

    [Fact]
    public async Task DeadLetterRecord_CarriesOriginalRecordAndFailureMetadata()
    {
        var headers = new Headers { { "trace-id", "abc"u8.ToArray() }, { DeadLetterHeaders.OriginalOffset, "999"u8.ToArray() } };
        var client = new FakeConsumerClient(Msg(2, 7, "m7", headers));
        using var cts = StopWhenIdle(client);
        var producer = new FakeRawProducer(client.Log);
        var consumer = new TestConsumer(client, [], _ => MessageOutcome.Retry, producer);

        await consumer.Run(cts.Token);

        var published = Assert.Single(producer.Published);
        Assert.Equal(DeadLetterTopic, published.Topic);
        Assert.Equal("key", published.Key);
        Assert.Equal("m7", published.Value);
        var record = published.Headers.ToDictionary(h => h.Key, h => System.Text.Encoding.UTF8.GetString(h.GetValueBytes()));
        Assert.Equal("abc", record["trace-id"]);
        Assert.Equal(Topic, record[DeadLetterHeaders.OriginalTopic]);
        Assert.Equal("2", record[DeadLetterHeaders.OriginalPartition]);
        Assert.Equal("7", record[DeadLetterHeaders.OriginalOffset]);
        Assert.Equal("5", record[DeadLetterHeaders.AttemptCount]);
        Assert.Equal(DeadLetterHeaders.RetryRequested, record[DeadLetterHeaders.ExceptionType]);
        Assert.Equal(FixedNow.UtcDateTime.ToString("O", System.Globalization.CultureInfo.InvariantCulture), record[DeadLetterHeaders.FailedAtUtc]);
        Assert.Equal(1, published.Headers.Count(h => h.Key == DeadLetterHeaders.OriginalOffset));
    }

    [Fact]
    public async Task DeadLetterRecord_ExceptionTypeIsRecorded_WithoutExceptionMessage()
    {
        var client = new FakeConsumerClient(Msg(0, 0, "m0"));
        using var cts = StopWhenIdle(client);
        var producer = new FakeRawProducer(client.Log);
        var consumer = new TestConsumer(client, [], _ => throw new InvalidOperationException("smtp user@example.com refused"), producer);

        await consumer.Run(cts.Token);

        var published = Assert.Single(producer.Published);
        var record = published.Headers.ToDictionary(h => h.Key, h => System.Text.Encoding.UTF8.GetString(h.GetValueBytes()));
        Assert.Equal(typeof(InvalidOperationException).FullName, record[DeadLetterHeaders.ExceptionType]);
        Assert.DoesNotContain(record.Values, v => v.Contains("example.com", StringComparison.Ordinal));
    }

    private static readonly DateTimeOffset FixedNow = new(2026, 11, 15, 10, 30, 0, TimeSpan.Zero);

    private static CancellationTokenSource StopWhenIdle(FakeConsumerClient client)
    {
        // Safety timeout so a regression cannot hang the test run.
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        client.OnIdle = () => cts.Cancel();
        return cts;
    }

    private static ConsumeResult<string, string> Msg(int partition, long offset, string? value, Headers? headers = null)
        => new()
        {
            Topic = Topic,
            Partition = new Partition(partition),
            Offset = new Offset(offset),
            Message = new Message<string, string> { Key = "key", Value = value!, Headers = headers },
        };

    /// <summary>
    /// In-memory consumer that models per-partition positions: Seek rewinds the position so
    /// the failed offset is delivered again. Pause only records the call; messages still
    /// arrive afterwards, which is the worst case (buffered messages) for the stop guard.
    /// Commits, pauses and dead-letter publishes are also written to one shared <see cref="Log"/>.
    /// </summary>
    private sealed class FakeConsumerClient : IKafkaConsumerClient
    {
        private readonly List<TopicPartition> _order = [];
        private readonly Dictionary<TopicPartition, List<ConsumeResult<string, string>>> _messages = [];
        private readonly Dictionary<TopicPartition, int> _positions = [];
        private int _next;

        public FakeConsumerClient(params ConsumeResult<string, string>[] messages)
        {
            foreach (var message in messages)
            {
                var tp = message.TopicPartition;
                if (!_messages.TryGetValue(tp, out var list))
                {
                    list = [];
                    _messages[tp] = list;
                    _order.Add(tp);
                    _positions[tp] = 0;
                }

                list.Add(message);
            }
        }

        public List<(int Partition, long Offset)> Commits { get; } = [];

        public List<(int Partition, long Offset)> Seeks { get; } = [];

        public List<int> Pauses { get; } = [];

        public List<string> Log { get; } = [];

        public Action? OnIdle { get; set; }

        public void Subscribe(string topic)
        {
        }

        public ConsumeResult<string, string>? Consume(TimeSpan timeout)
        {
            for (var i = 0; i < _order.Count; i++)
            {
                var tp = _order[(_next + i) % _order.Count];
                var list = _messages[tp];
                if (_positions[tp] < list.Count)
                {
                    _next = (_next + i + 1) % _order.Count;
                    return list[_positions[tp]++];
                }
            }

            OnIdle?.Invoke();
            return null;
        }

        public void Seek(TopicPartitionOffset position)
        {
            Seeks.Add((position.Partition.Value, position.Offset.Value));
            var list = _messages[position.TopicPartition];
            _positions[position.TopicPartition] = list.FindIndex(m => m.Offset.Value == position.Offset.Value);
        }

        public void Pause(TopicPartition partition)
        {
            Pauses.Add(partition.Partition.Value);
            Log.Add($"pause:{partition.Partition.Value}");
        }

        public void Commit(ConsumeResult<string, string> result)
        {
            Commits.Add((result.Partition.Value, result.Offset.Value));
            Log.Add($"commit:{result.Partition.Value}:{result.Offset.Value}");
        }

        public void Close()
        {
        }

        public void Dispose()
        {
        }
    }

    /// <summary>Dead-letter producer that records publishes, or throws <see cref="Failure"/> instead.</summary>
    private sealed class FakeRawProducer : IKafkaRawProducer
    {
        private readonly List<string> _log;

        public FakeRawProducer(List<string> log)
        {
            _log = log;
        }

        public List<(string Topic, string? Key, string? Value, Headers Headers)> Published { get; } = [];

        public Exception? Failure { get; init; }

        public Task ProduceAsync(string topic, string? key, string? value, Headers headers, CancellationToken ct)
        {
            Published.Add((topic, key, value, headers));
            _log.Add($"dlq:{value}");
            if (Failure is not null)
            {
                throw Failure;
            }

            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }
    }

    private sealed class FixedTimeProvider : TimeProvider
    {
        private readonly DateTimeOffset _now;

        public FixedTimeProvider(DateTimeOffset now)
        {
            _now = now;
        }

        public override DateTimeOffset GetUtcNow() => _now;
    }

    private sealed class TestConsumer : KafkaConsumerService
    {
        private readonly Func<string?, MessageOutcome> _decide;

        public TestConsumer(
            FakeConsumerClient client,
            List<TimeSpan> delays,
            Func<string?, MessageOutcome> decide,
            FakeRawProducer? producer = null,
            bool deadLetterEnabled = true)
            : base(
                Options.Create(new KafkaOptions { BootstrapServers = "fake:9092", DeadLetterEnabled = deadLetterEnabled }),
                TestLogger.Instance,
                _ => client,
                (delay, _) =>
                {
                    delays.Add(delay);
                    return Task.CompletedTask;
                },
                _ => producer ?? new FakeRawProducer(client.Log),
                new FixedTimeProvider(FixedNow))
        {
            _decide = decide;
        }

        public List<string> Handled { get; } = [];

        protected override string Topic => ConsumerLoopTests.Topic;

        protected override string GroupId => "test-group";

        public Task Run(CancellationToken ct) => ExecuteAsync(ct);

        protected override Task<MessageOutcome> HandleMessageAsync(string? key, string value, CancellationToken ct)
        {
            Handled.Add(value);
            return Task.FromResult(_decide(value));
        }

        public static bool TryParse<T>(string? json, out EventEnvelope<T>? envelope)
            => TryParseEnvelope(json!, out envelope);
    }
}

using System.Globalization;
using System.Text;
using Confluent.Kafka;
using SharedKernel.Kafka;

namespace SharedKernel.Tests;

/// <summary>Unit tests for the dead-letter envelope, topic naming, options and producer settings.</summary>
public class DeadLetterTests
{
    private static readonly DateTimeOffset FailedAt = new(2026, 11, 15, 10, 30, 0, TimeSpan.Zero);

    [Fact]
    public void BuildHeaders_AddsAllMetadataHeaders_WithExpectedValues()
    {
        var result = Result("job-events", partition: 3, offset: 42, headers: null);

        var headers = ToMap(DeadLetter.BuildHeaders(result, 5, "System.InvalidOperationException", FailedAt));

        Assert.Equal("job-events", headers[DeadLetterHeaders.OriginalTopic]);
        Assert.Equal("3", headers[DeadLetterHeaders.OriginalPartition]);
        Assert.Equal("42", headers[DeadLetterHeaders.OriginalOffset]);
        Assert.Equal("5", headers[DeadLetterHeaders.AttemptCount]);
        Assert.Equal("System.InvalidOperationException", headers[DeadLetterHeaders.ExceptionType]);
        Assert.Equal("2026-11-15T10:30:00.0000000Z", headers[DeadLetterHeaders.FailedAtUtc]);
        Assert.Equal(6, headers.Count);
    }

    [Fact]
    public void BuildHeaders_KeepsOriginalHeaders_AndDropsStaleMetadata()
    {
        var original = new Headers
        {
            { "trace-id", Encoding.UTF8.GetBytes("abc") },
            { DeadLetterHeaders.OriginalTopic, Encoding.UTF8.GetBytes("older-topic") },
            { DeadLetterHeaders.AttemptCount, Encoding.UTF8.GetBytes("99") },
        };
        var result = Result("job-events", 0, 1, original);

        var built = DeadLetter.BuildHeaders(result, 2, DeadLetterHeaders.RetryRequested, FailedAt);

        Assert.Equal(7, built.Count());
        var headers = ToMap(built);
        Assert.Equal("abc", headers["trace-id"]);
        Assert.Equal("job-events", headers[DeadLetterHeaders.OriginalTopic]);
        Assert.Equal("2", headers[DeadLetterHeaders.AttemptCount]);
        Assert.Equal(DeadLetterHeaders.RetryRequested, headers[DeadLetterHeaders.ExceptionType]);
    }

    [Fact]
    public void BuildHeaders_NullOriginalHeaders_IsAccepted()
    {
        var result = Result("job-events", 0, 0, headers: null);

        var built = DeadLetter.BuildHeaders(result, 1, "x", FailedAt);

        Assert.Equal(6, built.Count());
    }

    [Theory]
    [InlineData("job-events", ".dlq", "job-events.dlq")]
    [InlineData("job-events", ".deadletter", "job-events.deadletter")]
    public void TopicName_AppendsSuffix(string source, string suffix, string expected)
    {
        Assert.Equal(expected, DeadLetter.TopicName(source, suffix));
    }

    [Fact]
    public void TopicName_TooLong_Throws()
    {
        var source = new string('a', 246);

        Assert.Throws<InvalidOperationException>(() => DeadLetter.TopicName(source, ".dlq"));
    }

    [Fact]
    public void Options_DeadLetterDefaults_AreEnabledWithDlqSuffix()
    {
        var options = new KafkaOptions();

        Assert.True(options.DeadLetterEnabled);
        Assert.Equal(".dlq", options.DeadLetterTopicSuffix);
        options.Validate();
    }

    [Theory]
    [InlineData("")]
    [InlineData("/dlq")]
    [InlineData(" .dlq")]
    [InlineData(".dl q")]
    public void Options_InvalidSuffix_FailsValidation(string suffix)
    {
        var options = new KafkaOptions { DeadLetterTopicSuffix = suffix };

        Assert.Throws<InvalidOperationException>(() => options.Validate());
    }

    [Fact]
    public void ProducerConfig_UsesAcksAllAndIdempotence()
    {
        var config = KafkaProducerService.CreateConfig(new KafkaOptions { BootstrapServers = " localhost:9092 " });

        Assert.Equal("localhost:9092", config.BootstrapServers);
        Assert.Equal(Acks.All, config.Acks);
        Assert.True(config.EnableIdempotence);
        Assert.Equal(10_000, config.MessageTimeoutMs);
    }

    [Fact]
    public void HeaderNames_AreStableContract()
    {
        Assert.Equal("dlq-original-topic", DeadLetterHeaders.OriginalTopic);
        Assert.Equal("dlq-original-partition", DeadLetterHeaders.OriginalPartition);
        Assert.Equal("dlq-original-offset", DeadLetterHeaders.OriginalOffset);
        Assert.Equal("dlq-exception-type", DeadLetterHeaders.ExceptionType);
        Assert.Equal("dlq-attempt-count", DeadLetterHeaders.AttemptCount);
        Assert.Equal("dlq-failed-at-utc", DeadLetterHeaders.FailedAtUtc);
    }

    private static ConsumeResult<string, string> Result(string topic, int partition, long offset, Headers? headers)
        => new()
        {
            Topic = topic,
            Partition = new Partition(partition),
            Offset = new Offset(offset),
            Message = new Message<string, string> { Key = "key", Value = "value", Headers = headers },
        };

    private static Dictionary<string, string> ToMap(Headers headers)
        => headers.ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.GetValueBytes()));
}

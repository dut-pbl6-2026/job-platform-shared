using System.Text.Json;
using Microsoft.Extensions.Options;
using SharedKernel.Events;
using SharedKernel.Kafka;

namespace SharedKernel.Tests;

public class EnvelopeTests
{
    private static JobCreatedEvent SampleJob() => new(
        Guid.NewGuid(), "Senior .NET Dev", "Experienced .NET",
        Guid.NewGuid(), "ABC", "Da Nang",
        20000000m, 40000000m, "VND",
        null, "CNTT", "FullTime", "Mid", Guid.NewGuid(),
        "C#", "Lunch", "Active", DateTime.UtcNow);

    private static ApplicationSubmittedEvent SampleSubmitted() => new(
        Guid.NewGuid(), Guid.NewGuid(), "Senior .NET Dev", Guid.NewGuid(), DateTime.UtcNow);

    private static ApplicationStatusChangedEvent SampleStatusChanged() => new(
        Guid.NewGuid(), Guid.NewGuid(), "Senior .NET Dev", Guid.NewGuid(),
        "pending", "reviewed", Guid.NewGuid(), DateTime.UtcNow);

    [Fact]
    public void RoundTrip_JobCreated_PreservesPayload()
    {
        var envelope = EventEnvelope<JobCreatedEvent>.Create(JobEventTypes.Created, SampleJob());

        var json = JsonSerializer.Serialize(envelope, KafkaJson.Options);
        var back = JsonSerializer.Deserialize<EventEnvelope<JobCreatedEvent>>(json, KafkaJson.Options);

        Assert.Equal(envelope, back);
    }

    [Fact]
    public void RoundTrip_ApplicationSubmitted_PreservesPayload()
    {
        var envelope = EventEnvelope<ApplicationSubmittedEvent>.Create(ApplicationEventTypes.Submitted, SampleSubmitted());

        var json = JsonSerializer.Serialize(envelope, KafkaJson.Options);
        var back = JsonSerializer.Deserialize<EventEnvelope<ApplicationSubmittedEvent>>(json, KafkaJson.Options);

        Assert.Equal(envelope, back);
    }

    [Fact]
    public void RoundTrip_ApplicationStatusChanged_PreservesPayload()
    {
        var envelope = EventEnvelope<ApplicationStatusChangedEvent>.Create(ApplicationEventTypes.StatusChanged, SampleStatusChanged());

        var json = JsonSerializer.Serialize(envelope, KafkaJson.Options);
        var back = JsonSerializer.Deserialize<EventEnvelope<ApplicationStatusChangedEvent>>(json, KafkaJson.Options);

        Assert.Equal(envelope, back);
    }

    [Fact]
    public void WireFormat_IsCamelCase()
    {
        var envelope = EventEnvelope<JobCreatedEvent>.Create(JobEventTypes.Created, SampleJob());

        var json = JsonSerializer.Serialize(envelope, KafkaJson.Options);

        Assert.Contains("\"eventType\"", json);
        Assert.Contains("\"payload\"", json);
        Assert.DoesNotContain("\"EventType\"", json);
    }

    [Fact]
    public void PascalCaseProducer_StillBinds()
    {
        var envelope = EventEnvelope<JobCreatedEvent>.Create(JobEventTypes.Created, SampleJob());
        var pascal = JsonSerializer.Serialize(envelope);

        var back = JsonSerializer.Deserialize<EventEnvelope<JobCreatedEvent>>(pascal, KafkaJson.Options);

        Assert.Equal(envelope, back);
    }

    [Fact]
    public void UnknownFields_AreIgnored()
    {
        var envelope = EventEnvelope<JobCreatedEvent>.Create(JobEventTypes.Created, SampleJob());
        var json = JsonSerializer.Serialize(envelope, KafkaJson.Options);
        json = json.Insert(json.Length - 1, ",\"unknownFutureField\":123");

        var back = JsonSerializer.Deserialize<EventEnvelope<JobCreatedEvent>>(json, KafkaJson.Options);

        Assert.Equal(envelope, back);
    }

    [Fact]
    public void Create_EmptyEventType_Throws()
    {
        Assert.Throws<ArgumentException>(() => EventEnvelope<JobCreatedEvent>.Create("  ", SampleJob()));
    }

    [Fact]
    public void Create_NullPayload_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => EventEnvelope<JobCreatedEvent>.Create(JobEventTypes.Created, null!));
    }

    [Fact]
    public void TryParse_ValidJson_ReturnsTrue()
    {
        var envelope = EventEnvelope<JobCreatedEvent>.Create(JobEventTypes.Created, SampleJob());
        var json = JsonSerializer.Serialize(envelope, KafkaJson.Options);

        var ok = ExposedConsumer.Parse(json, out EventEnvelope<JobCreatedEvent>? parsed);

        Assert.True(ok);
        Assert.Equal(envelope, parsed);
    }

    [Fact]
    public void TryParse_InvalidJson_ReturnsFalse()
    {
        var ok = ExposedConsumer.Parse<JobCreatedEvent>("not-json{{{", out var parsed);

        Assert.False(ok);
        Assert.Null(parsed);
    }

    [Fact]
    public void TryParse_NullPayload_ReturnsFalse()
    {
        var json = "{\"eventId\":\"" + Guid.NewGuid()
            + "\",\"eventType\":\"job.created\",\"version\":1"
            + ",\"timestamp\":\"2026-01-01T00:00:00Z\",\"payload\":null}";

        var ok = ExposedConsumer.Parse<JobCreatedEvent>(json, out var parsed);

        Assert.False(ok);
        Assert.Null(parsed);
    }

    private sealed class ExposedConsumer : KafkaConsumerService
    {
        public ExposedConsumer()
            : base(Options.Create(new KafkaOptions()), TestLogger.Instance)
        {
        }

        protected override string Topic => "test-topic";

        protected override string GroupId => "test-group";

        protected override Task<MessageOutcome> HandleMessageAsync(string? key, string value, CancellationToken ct)
            => Task.FromResult(MessageOutcome.Handled);

        public static bool Parse<T>(string json, out EventEnvelope<T>? envelope)
            => TryParseEnvelope(json, out envelope);
    }
}

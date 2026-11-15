using System.Text.Json;
using SharedKernel.Events;
using SharedKernel.Kafka;

namespace SharedKernel.Tests;

/// <summary>
/// Contract layer: the wire shape of the shared envelope and payloads is part of the
/// cross-service contract. These tests pin field names, JSON types and event type names.
/// </summary>
public class ContractTests
{
    private static readonly Guid Id = Guid.Parse("11111111-2222-3333-4444-555555555555");
    private static readonly DateTime At = new(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);

    private static readonly JsonValueKind S = JsonValueKind.String;
    private static readonly JsonValueKind N = JsonValueKind.Number;
    private static readonly JsonValueKind O = JsonValueKind.Object;

    [Fact]
    public void EventTypeNames_MatchDocumentedValues()
    {
        Assert.Equal("job.created", JobEventTypes.Created);
        Assert.Equal("job.updated", JobEventTypes.Updated);
        Assert.Equal("job.deleted", JobEventTypes.Deleted);
        Assert.Equal("application.submitted", ApplicationEventTypes.Submitted);
        Assert.Equal("application.status_changed", ApplicationEventTypes.StatusChanged);
    }

    [Fact]
    public void Envelope_WireShape_HasExactFieldsAndTypes()
    {
        var envelope = new EventEnvelope<JobDeletedEvent>(Id, JobEventTypes.Deleted, 1, At, new JobDeletedEvent(Id, At));

        var root = Parse(envelope);

        AssertShape(root,
            F("eventId", S), F("eventType", S), F("version", N), F("timestamp", S), F("payload", O));
        Assert.Equal(Id, root.GetProperty("eventId").GetGuid());
        Assert.Equal(At, root.GetProperty("timestamp").GetDateTime());
    }

    [Fact]
    public void Create_SetsVersionOneAndEventType()
    {
        var envelope = EventEnvelope<JobDeletedEvent>.Create(JobEventTypes.Deleted, new JobDeletedEvent(Id, At));

        Assert.Equal(1, envelope.Version);
        Assert.Equal(JobEventTypes.Deleted, envelope.EventType);
        Assert.NotEqual(Guid.Empty, envelope.EventId);
    }

    [Fact]
    public void JobDeleted_Payload_HasExactFieldsAndTypes()
    {
        var payload = Payload(new JobDeletedEvent(Id, At));

        AssertShape(payload, F("jobId", S), F("occurredAt", S));
    }

    [Fact]
    public void JobCreated_Payload_HasExactFieldsAndTypes()
    {
        var payload = Payload(JobCreated(createdAt: At));

        AssertShape(payload, JobPayloadShape());
        Assert.Equal(At, payload.GetProperty("createdAt").GetDateTime());
    }

    [Fact]
    public void JobUpdated_Payload_HasSameShapeAsJobCreated()
    {
        var updated = new JobUpdatedEvent(
            Id, "Title", "Description", Id, "ABC", "Da Nang", 1m, 2m, "VND",
            null, null, "FullTime", "Mid", Id, null, null, "Active", At, null);

        var payload = Payload(updated);

        AssertShape(payload, JobPayloadShape());
        Assert.Equal(JsonValueKind.Null, payload.GetProperty("createdAt").ValueKind);
    }

    [Fact]
    public void ApplicationSubmitted_Payload_HasExactFieldsAndTypes_WithoutPii()
    {
        var payload = Payload(new ApplicationSubmittedEvent(Id, Id, "Title", Id, At));

        AssertShape(payload,
            F("applicationId", S), F("jobId", S), F("jobTitle", S), F("applicantId", S), F("occurredAt", S));
    }

    [Fact]
    public void ApplicationStatusChanged_Payload_HasExactFieldsAndTypes_WithoutPii()
    {
        var payload = Payload(new ApplicationStatusChangedEvent(Id, Id, "Title", Id, "pending", "reviewed", Id, At));

        AssertShape(payload,
            F("applicationId", S), F("jobId", S), F("jobTitle", S), F("applicantId", S),
            F("previousStatus", S), F("newStatus", S), F("changedBy", S), F("occurredAt", S));
    }

    [Fact]
    public void JobCreated_WireJson_BindsToPayload()
    {
        // Hand-written wire message as a producer on the documented contract would send it.
        const string json = """
            {"eventId":"11111111-2222-3333-4444-555555555555","eventType":"job.created","version":1,
             "timestamp":"2026-01-02T03:04:05Z",
             "payload":{"jobId":"11111111-2222-3333-4444-555555555555","title":"Senior .NET",
               "description":"d","companyId":"11111111-2222-3333-4444-555555555555","companyName":"ABC",
               "location":"Da Nang","salaryMin":20000000,"salaryMax":null,"currency":"VND",
               "categoryId":null,"categoryName":"IT","employmentType":"FullTime",
               "experienceLevel":"Mid","recruiterId":"11111111-2222-3333-4444-555555555555",
               "requirements":"C#","benefits":null,"status":"Active",
               "occurredAt":"2026-01-02T03:04:05Z","createdAt":"2026-01-01T00:00:00Z"}}
            """;

        var envelope = JsonSerializer.Deserialize<EventEnvelope<JobCreatedEvent>>(json, KafkaJson.Options);

        Assert.NotNull(envelope);
        Assert.Equal("job.created", envelope!.EventType);
        Assert.Equal(1, envelope.Version);
        Assert.Equal("Senior .NET", envelope.Payload.Title);
        Assert.Equal(20000000m, envelope.Payload.SalaryMin);
        Assert.Null(envelope.Payload.SalaryMax);
        Assert.Equal(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc), envelope.Payload.CreatedAt);
    }

    private static JsonElement Parse<T>(EventEnvelope<T> envelope)
        => JsonDocument.Parse(JsonSerializer.Serialize(envelope, KafkaJson.Options)).RootElement.Clone();

    private static JsonElement Payload<T>(T payload)
        => Parse(EventEnvelope<T>.Create("test.type", payload)).GetProperty("payload");

    private static JobCreatedEvent JobCreated(DateTime createdAt) => new(
        Id, "Title", "Description", Id, "ABC", "Da Nang",
        20000000m, 40000000m, "VND", null, "CNTT", "FullTime", "Mid", Id,
        "C#", "Lunch", "Active", At, createdAt);

    private static Field F(string name, JsonValueKind kind, bool nullable = false) => new(name, kind, nullable);

    private static Field[] JobPayloadShape() =>
    [
        F("jobId", S), F("title", S), F("description", S), F("companyId", S), F("companyName", S),
        F("location", S), F("salaryMin", N, true), F("salaryMax", N, true), F("currency", S),
        F("categoryId", S, true), F("categoryName", S, true), F("employmentType", S), F("experienceLevel", S),
        F("recruiterId", S), F("requirements", S, true), F("benefits", S, true), F("status", S),
        F("occurredAt", S), F("createdAt", S, true),
    ];

    /// <summary>Asserts the exact property set of an object, and each property's JSON type.</summary>
    private static void AssertShape(JsonElement element, params Field[] expected)
    {
        var actual = element.EnumerateObject()
            .Select(p => (Name: p.Name, Value: p.Value))
            .OrderBy(p => p.Name, StringComparer.Ordinal)
            .ToArray();
        var wanted = expected.OrderBy(p => p.Name, StringComparer.Ordinal).ToArray();

        Assert.Equal(wanted.Select(p => p.Name), actual.Select(p => p.Name));
        foreach (var field in wanted)
        {
            var value = actual.Single(p => p.Name == field.Name).Value;
            var ok = value.ValueKind == field.Kind || (field.Nullable && value.ValueKind == JsonValueKind.Null);
            Assert.True(ok, $"field '{field.Name}' expected {field.Kind} but was {value.ValueKind}");
        }
    }

    private readonly record struct Field(string Name, JsonValueKind Kind, bool Nullable);
}

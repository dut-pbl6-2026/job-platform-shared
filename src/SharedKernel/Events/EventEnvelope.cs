namespace SharedKernel.Events;

/// <summary>
/// Generic Kafka message envelope. Carries schema version and routing metadata so
/// consumers can evolve independently: unknown event types are skipped, unknown JSON
/// fields are ignored and property names bind case-insensitively
/// (see <c>SharedKernel.Kafka.KafkaJson</c>), version bumps stay backward compatible.
/// </summary>
/// <typeparam name="T">Payload type (e.g. <see cref="JobCreatedEvent"/>).</typeparam>
/// <param name="EventId">Unique message id for dedupe and manual replay tracing.</param>
/// <param name="EventType">Event name (see <see cref="JobEventTypes"/> / <see cref="ApplicationEventTypes"/>).</param>
/// <param name="Version">Schema version, starts at 1.</param>
/// <param name="Timestamp">UTC time the envelope was produced.</param>
/// <param name="Payload">Event payload.</param>
public sealed record EventEnvelope<T>(
    Guid EventId,
    string EventType,
    int Version,
    DateTime Timestamp,
    T Payload)
{
    /// <summary>Creates a v1 envelope with a new <see cref="EventId"/> and UTC timestamp.</summary>
    /// <param name="eventType">Event name (see <see cref="JobEventTypes"/> / <see cref="ApplicationEventTypes"/>). Must not be empty.</param>
    /// <param name="payload">Event payload. Must not be null.</param>
    /// <returns>Envelope ready to publish via <c>KafkaProducerService</c>.</returns>
    /// <exception cref="ArgumentException"><paramref name="eventType"/> is empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="payload"/> is null.</exception>
    public static EventEnvelope<T> Create(string eventType, T payload)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventType);
        ArgumentNullException.ThrowIfNull(payload);
        return new(Guid.NewGuid(), eventType.Trim(), 1, DateTime.UtcNow, payload);
    }
}

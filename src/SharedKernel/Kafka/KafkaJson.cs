using System.Text.Json;

namespace SharedKernel.Kafka;

/// <summary>
/// Single JSON policy for all Kafka envelopes (P0 fix): producer serializes and every
/// consumer deserializes with the same options, so a message written by
/// <c>KafkaProducerService</c> always binds on the consuming side. Camel-case on the
/// wire, case-insensitive on read (tolerates producers on older/other SDK defaults).
/// </summary>
public static class KafkaJson
{
    /// <summary>Shared serializer options. Do not mutate after first use.</summary>
    public static JsonSerializerOptions Options { get; } = new(JsonSerializerDefaults.Web)
    {
        PropertyNameCaseInsensitive = true,
    };
}

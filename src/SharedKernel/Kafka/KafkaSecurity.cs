using Confluent.Kafka;

namespace SharedKernel.Kafka;

// Shared transport mapping for producer and consumer (review P1).
// Callers must run KafkaOptions.Validate() first; this method assumes valid input.
internal static class KafkaSecurity
{
    internal static void Apply(ClientConfig config, KafkaOptions kafka)
    {
        if (!string.IsNullOrWhiteSpace(kafka.SecurityProtocol))
        {
            config.SecurityProtocol = Enum.Parse<SecurityProtocol>(kafka.SecurityProtocol.Trim(), ignoreCase: true);
        }
        else
        {
            // Auto: encrypted SASL in prod, plaintext only for local dev without credentials.
            config.SecurityProtocol = string.IsNullOrWhiteSpace(kafka.SaslUsername)
                ? SecurityProtocol.Plaintext
                : SecurityProtocol.SaslSsl;
        }

        if (string.IsNullOrWhiteSpace(kafka.SaslUsername))
        {
            if (!string.IsNullOrWhiteSpace(kafka.SslCaLocation))
            {
                config.SslCaLocation = kafka.SslCaLocation.Trim();
            }

            return;
        }

        config.SaslMechanism = string.IsNullOrWhiteSpace(kafka.SaslMechanism)
            ? SaslMechanism.Plain
            : Enum.Parse<SaslMechanism>(kafka.SaslMechanism.Trim(), ignoreCase: true);
        config.SaslUsername = kafka.SaslUsername;
        config.SaslPassword = kafka.SaslPassword;

        if (!string.IsNullOrWhiteSpace(kafka.SslCaLocation))
        {
            config.SslCaLocation = kafka.SslCaLocation.Trim();
        }
    }
}

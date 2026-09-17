using Confluent.Kafka;
using Microsoft.Extensions.Options;
using SharedKernel.Kafka;

namespace SharedKernel.Tests;

public class KafkaRuntimeTests
{
    [Fact]
    public void Security_Default_MapsToPlaintext()
    {
        var config = new ProducerConfig();

        KafkaSecurity.Apply(config, new KafkaOptions());

        Assert.Equal(SecurityProtocol.Plaintext, config.SecurityProtocol);
    }

    [Fact]
    public void Security_Username_MapsToSaslSsl()
    {
        var config = new ConsumerConfig();
        var options = new KafkaOptions { SaslUsername = "svc", SaslPassword = "secret" };

        KafkaSecurity.Apply(config, options);

        Assert.Equal(SecurityProtocol.SaslSsl, config.SecurityProtocol);
        Assert.Equal("svc", config.SaslUsername);
        Assert.Equal("secret", config.SaslPassword);
        Assert.Equal(SaslMechanism.Plain, config.SaslMechanism);
    }

    [Fact]
    public void Security_ExplicitProtocol_IsHonored()
    {
        var config = new ProducerConfig();
        var options = new KafkaOptions
        {
            SaslUsername = "svc",
            SaslPassword = "secret",
            SecurityProtocol = "SaslPlaintext",
            SaslMechanism = "ScramSha256",
        };

        KafkaSecurity.Apply(config, options);

        Assert.Equal(SecurityProtocol.SaslPlaintext, config.SecurityProtocol);
        Assert.Equal(SaslMechanism.ScramSha256, config.SaslMechanism);
    }

    [Fact]
    public void Security_CaLocation_IsApplied()
    {
        var ca = Path.GetTempFileName();
        try
        {
            var config = new ConsumerConfig();
            var options = new KafkaOptions { SslCaLocation = ca };

            KafkaSecurity.Apply(config, options);

            Assert.Equal(ca, config.SslCaLocation);
        }
        finally
        {
            File.Delete(ca);
        }
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 4)]
    [InlineData(4, 8)]
    [InlineData(5, 16)]
    [InlineData(6, 30)]
    [InlineData(100, 30)]
    public void RetryDelay_BackoffsExponentiallyThenCaps(int failures, int expectedSeconds)
    {
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), KafkaConsumerService.RetryDelay(failures));
    }

    [Fact]
    public void Producer_EmptyBootstrap_Throws()
    {
        Assert.Throws<InvalidOperationException>(() => new KafkaProducerService(
            Options.Create(new KafkaOptions()),
            TestLogger<KafkaProducerService>.Instance));
    }

    [Fact]
    public void Producer_ValidOptions_BuildsWithoutBroker()
    {
        using var producer = new KafkaProducerService(
            Options.Create(new KafkaOptions { BootstrapServers = "localhost:9092" }),
            TestLogger<KafkaProducerService>.Instance);

        Assert.NotNull(producer);
    }
}

using SharedKernel.Kafka;

namespace SharedKernel.Tests;

public class KafkaOptionsTests
{
    [Fact]
    public void DefaultOptions_AreValid()
    {
        var options = new KafkaOptions();

        var ex = Record.Exception(() => options.Validate());

        Assert.Null(ex);
    }

    [Fact]
    public void UsernameWithoutPassword_Throws()
    {
        var options = new KafkaOptions { SaslUsername = "svc" };

        Assert.Throws<InvalidOperationException>(() => options.Validate());
    }

    [Fact]
    public void UnknownProtocol_Throws()
    {
        var options = new KafkaOptions { SecurityProtocol = "SuperSsl" };

        Assert.Throws<InvalidOperationException>(() => options.Validate());
    }

    [Fact]
    public void UnknownMechanism_Throws()
    {
        var options = new KafkaOptions { SaslMechanism = "Magic" };

        Assert.Throws<InvalidOperationException>(() => options.Validate());
    }

    [Fact]
    public void MissingCaFile_Throws()
    {
        var options = new KafkaOptions { SslCaLocation = "no-such-ca.pem" };

        Assert.Throws<InvalidOperationException>(() => options.Validate());
    }

    [Fact]
    public void ValidProdOptions_Pass()
    {
        var ca = Path.GetTempFileName();
        try
        {
            var options = new KafkaOptions
            {
                BootstrapServers = "broker:9092",
                SaslUsername = "svc",
                SaslPassword = "secret",
                SecurityProtocol = "SaslSsl",
                SaslMechanism = "ScramSha256",
                SslCaLocation = ca,
            };

            var ex = Record.Exception(() => options.Validate());

            Assert.Null(ex);
        }
        finally
        {
            File.Delete(ca);
        }
    }
}

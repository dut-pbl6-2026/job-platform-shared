using SharedKernel.Kafka;

namespace SharedKernel.Tests;

/// <summary>Unit layer: pure retry budget and backoff decisions used by the consume loop.</summary>
public class RetryPolicyTests
{
    [Fact]
    public void Budget_IsFiveHandlerInvocations()
    {
        Assert.Equal(5, RetryPolicy.MaxAttempts);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(3, 4)]
    [InlineData(4, 8)]
    public void Decide_BeforeBudgetIsExhausted_RetriesWithBackoff(int attempt, int expectedSeconds)
    {
        var decision = RetryPolicy.Decide(attempt);

        Assert.True(decision.Retry);
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), decision.Delay);
    }

    [Fact]
    public void Decide_LastAttempt_StopsWithoutDelay()
    {
        var decision = RetryPolicy.Decide(RetryPolicy.MaxAttempts);

        Assert.False(decision.Retry);
        Assert.Equal(TimeSpan.Zero, decision.Delay);
    }

    [Fact]
    public void Decide_BeyondBudget_StaysStopped()
    {
        Assert.False(RetryPolicy.Decide(RetryPolicy.MaxAttempts + 1).Retry);
    }

    [Fact]
    public void Decide_NonPositiveAttempt_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => RetryPolicy.Decide(0));
    }

    [Fact]
    public void Budget_TotalBackoffBeforeStop_IsFifteenSeconds()
    {
        var total = TimeSpan.Zero;
        for (var attempt = 1; attempt < RetryPolicy.MaxAttempts; attempt++)
        {
            total += RetryPolicy.Decide(attempt).Delay;
        }

        Assert.Equal(TimeSpan.FromSeconds(15), total);
    }

    [Fact]
    public void RetryDelay_IsMonotonicAndBoundedBetweenOneAndThirtySeconds()
    {
        var previous = TimeSpan.Zero;
        for (var failures = 0; failures <= 1000; failures++)
        {
            var delay = KafkaConsumerService.RetryDelay(failures);

            Assert.InRange(delay, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30));
            Assert.True(delay >= previous, $"delay decreased at failures={failures}");
            previous = delay;
        }
    }
}

namespace SharedKernel.Kafka;

/// <summary>
/// Handler decision for one consumed message. This is the poison-message contract
/// (review P1): the base class owns commit/skip/retry, the derived class only decides.
/// </summary>
public enum MessageOutcome
{
    /// <summary>Handled successfully: commit the offset, continue.</summary>
    Handled = 0,

    /// <summary>
    /// Poison or unknown message (bad JSON, unknown event type, unsupported version):
    /// log and commit to skip. Never return this for transient errors — they would
    /// be silently dropped instead of redelivered.
    /// </summary>
    Skip = 1,

    /// <summary>Transient failure (DB/ES/SMTP down): do not commit, redeliver after backoff.</summary>
    Retry = 2,
}

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

    /// <summary>
    /// Transient failure (DB/ES/SMTP down): seek back and redeliver the same message after
    /// backoff. After the bounded retry budget the message is published to the dead-letter topic
    /// and the partition moves on; if that publish fails (or dead-lettering is disabled) the
    /// partition is stopped, uncommitted.
    /// </summary>
    Retry = 2,
}

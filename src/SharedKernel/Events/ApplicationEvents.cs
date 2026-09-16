namespace SharedKernel.Events;

/// <summary>Event type names published on the <c>application-events</c> Kafka topic (SRS 8.5).</summary>
public static class ApplicationEventTypes
{
    /// <summary>Application submitted.</summary>
    public const string Submitted = "application.submitted";
    /// <summary>Application status changed.</summary>
    public const string StatusChanged = "application.status_changed";
}

/// <summary>
/// Payload of <c>application.submitted</c>. No PII and no CV content: ids only so the
/// consumer resolves applicant email from its own DB (SEC-05).
/// Kafka key must be <paramref name="ApplicationId"/> (plan 2.2.1).
/// </summary>
/// <param name="ApplicationId">Application primary key. Also the Kafka message key.</param>
/// <param name="JobId">Applied job id.</param>
/// <param name="JobTitle">Denormalized job title for the email template.</param>
/// <param name="ApplicantId">Candidate id; consumer resolves the email address.</param>
/// <param name="OccurredAt">UTC time the application was submitted.</param>
public sealed record ApplicationSubmittedEvent(
    Guid ApplicationId,
    Guid JobId,
    string JobTitle,
    Guid ApplicantId,
    DateTime OccurredAt);

/// <summary>
/// Payload of <c>application.status_changed</c>. Kafka key must be
/// <paramref name="ApplicationId"/> so submitted then status changes of one application
/// stay ordered on the same partition (plan 2.2.1).
/// </summary>
/// <param name="ApplicationId">Application primary key. Also the Kafka message key.</param>
/// <param name="JobId">Applied job id.</param>
/// <param name="JobTitle">Denormalized job title for the email template.</param>
/// <param name="ApplicantId">Candidate id; consumer resolves the email address.</param>
/// <param name="PreviousStatus">Status before the transition.</param>
/// <param name="NewStatus">Status after the transition.</param>
/// <param name="ChangedBy">User id that performed the transition.</param>
/// <param name="OccurredAt">UTC time the status changed.</param>
public sealed record ApplicationStatusChangedEvent(
    Guid ApplicationId,
    Guid JobId,
    string JobTitle,
    Guid ApplicantId,
    string PreviousStatus,
    string NewStatus,
    Guid ChangedBy,
    DateTime OccurredAt);

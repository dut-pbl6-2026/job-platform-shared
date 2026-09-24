namespace SharedKernel.Events;

/// <summary>Event type names published on the <c>job-events</c> Kafka topic (SRS 8.5).</summary>
public static class JobEventTypes
{
    /// <summary>Job created.</summary>
    public const string Created = "job.created";
    /// <summary>Job updated.</summary>
    public const string Updated = "job.updated";
    /// <summary>Job deleted.</summary>
    public const string Deleted = "job.deleted";
}

/// <summary>
/// Payload of <c>job.created</c>. No PII: ids plus search metadata only (SEC-05).
/// Kafka key must be <paramref name="JobId"/> so all events of one job land on the
/// same partition in order (plan 2.2.1).
/// </summary>
/// <param name="JobId">Job primary key. Also the Kafka message key.</param>
/// <param name="Title">Job title.</param>
/// <param name="Description">Job description (needed for ES indexing).</param>
/// <param name="CompanyId">Hiring company id.</param>
/// <param name="CompanyName">Denormalized company name for search display.</param>
/// <param name="Location">Normalized location string.</param>
/// <param name="SalaryMin">Minimum salary in <paramref name="Currency"/>.</param>
/// <param name="SalaryMax">Maximum salary in <paramref name="Currency"/>.</param>
/// <param name="Currency">Salary currency code (default VND).</param>
/// <param name="CategoryId">Category id, if any.</param>
/// <param name="CategoryName">Denormalized category name, if any.</param>
/// <param name="EmploymentType">Employment type string (e.g. FullTime).</param>
/// <param name="ExperienceLevel">Experience level string (e.g. Entry).</param>
/// <param name="RecruiterId">Owner recruiter id.</param>
/// <param name="Requirements">Job requirements text, if any.</param>
/// <param name="Benefits">Job benefits text, if any.</param>
/// <param name="Status">Job status string at publish time.</param>
/// <param name="OccurredAt">UTC time the domain change happened.</param>
public sealed record JobCreatedEvent(
    Guid JobId,
    string Title,
    string Description,
    Guid CompanyId,
    string CompanyName,
    string Location,
    decimal? SalaryMin,
    decimal? SalaryMax,
    string Currency,
    Guid? CategoryId,
    string? CategoryName,
    string EmploymentType,
    string ExperienceLevel,
    Guid RecruiterId,
    string? Requirements,
    string? Benefits,
    string Status,
    DateTime OccurredAt);

/// <summary>
/// Payload of <c>job.updated</c>. Same shape as <see cref="JobCreatedEvent"/> so the
/// consumer can upsert with one code path. Kafka key must be <paramref name="JobId"/>.
/// </summary>
/// <param name="JobId">Job primary key. Also the Kafka message key.</param>
/// <param name="Title">Job title.</param>
/// <param name="Description">Job description (needed for ES indexing).</param>
/// <param name="CompanyId">Hiring company id.</param>
/// <param name="CompanyName">Denormalized company name for search display.</param>
/// <param name="Location">Normalized location string.</param>
/// <param name="SalaryMin">Minimum salary in <paramref name="Currency"/>.</param>
/// <param name="SalaryMax">Maximum salary in <paramref name="Currency"/>.</param>
/// <param name="Currency">Salary currency code (default VND).</param>
/// <param name="CategoryId">Category id, if any.</param>
/// <param name="CategoryName">Denormalized category name, if any.</param>
/// <param name="EmploymentType">Employment type string (e.g. FullTime).</param>
/// <param name="ExperienceLevel">Experience level string (e.g. Entry).</param>
/// <param name="RecruiterId">Owner recruiter id.</param>
/// <param name="Requirements">Job requirements text, if any.</param>
/// <param name="Benefits">Job benefits text, if any.</param>
/// <param name="Status">Job status string at publish time.</param>
/// <param name="OccurredAt">UTC time the domain change happened.</param>
public sealed record JobUpdatedEvent(
    Guid JobId,
    string Title,
    string Description,
    Guid CompanyId,
    string CompanyName,
    string Location,
    decimal? SalaryMin,
    decimal? SalaryMax,
    string Currency,
    Guid? CategoryId,
    string? CategoryName,
    string EmploymentType,
    string ExperienceLevel,
    Guid RecruiterId,
    string? Requirements,
    string? Benefits,
    string Status,
    DateTime OccurredAt);

/// <summary>Payload of <c>job.deleted</c>. Kafka key must be <paramref name="JobId"/>.</summary>
/// <param name="JobId">Deleted job primary key. Also the Kafka message key.</param>
/// <param name="OccurredAt">UTC time the delete happened.</param>
public sealed record JobDeletedEvent(
    Guid JobId,
    DateTime OccurredAt);

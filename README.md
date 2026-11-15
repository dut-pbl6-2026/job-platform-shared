# job-platform-shared

.NET Class Library SharedKernel — **Vietnam Job Platform** (`pbl6`) under [`dut-pbl6-2026`](https://github.com/dut-pbl6-2026). `PackageId JobPlatform.SharedKernel 0.3.1`.

## Prerequisites

- `mise` https://mise.jdx.dev
- `git` + `gh` `gh auth login`
- `dotnet 10.0.100` via `mise` — `mise trust && mise install`

See `AGENTS.md` for shell activation (`mise activate`) and agent `mise exec` notes.

## Clone

```bash
gh repo clone dut-pbl6-2026/job-platform-shared
cd job-platform-shared
```

## Setup

```bash
mise trust && mise install
mise run build
mise run verify  # 1 nupkg in artifacts
```

No `env` needed for this repo (library). For full stack `env` see `job-platform-infra/envs/.env.dev.example` + `mise run sync-env` in infra.

## Build

```bash
mise run build   # dotnet build --warnaserror
mise run test    # dotnet test (Events/Kafka suite)
mise run format  # dotnet format --verify-no-changes
mise run pack    # dotnet pack -o ./artifacts
mise run verify  # build + test + format + pack
```

- `src/SharedKernel` `Result<T>` `Entity` `ValueObject` `JwtOptions`.
- `src/SharedKernel/Events` `JobEvents` (`job.created|updated|deleted`) `ApplicationEvents` (`application.submitted|status_changed`) `EventEnvelope<T>` — no PII (`SEC-05`).
- `src/SharedKernel/Kafka` `KafkaOptions` (bootstrap + SASL) `KafkaProducerService` (key-required produce, idempotent) `KafkaConsumerService` (BackgroundService base, manual commit, idle when unconfigured).
- `GenerateDocumentationFile` true — XML docs required.

Kafka rules: partition key is mandatory (`JobId` for `job-events`, `ApplicationId` for `application-events`); wire JSON is camelCase via `KafkaJson.Options` (case-insensitive on read).

Delivery contract (at-least-once): the consumer commits only after `Handled`/`Skip`; `Retry` (or throw) seeks back to the failed offset and redelivers it after 1s, 2s, 4s, 8s backoff (5 attempts in total). If all 5 attempts fail, the failed message is published to the dead-letter topic (see below) and only then is its offset committed, so the partition moves on. If the dead-letter publish fails, or dead-lettering is disabled, the partition is paused and its offset stays uncommitted until the service restarts (error logged with topic, partition and offset). Handlers must dedupe on `EventEnvelope.EventId` — e.g. unique `(application_id, event_type, status_snapshot)` in notif `email_logs`, ES `_id` = job id in search. Poison/unknown messages must return `Skip` (commit past them, not dead-lettered), never `Retry`.

### Dead-letter topic

- Name: `<original topic>` + `DeadLetterTopicSuffix` (default `.dlq`), e.g. `job-events.dlq`, `application-events.dlq`. The suffix may contain only ASCII letters, digits, `.`, `_`, `-`; the full name must be at most 249 characters. An invalid name stops the consumer at startup.
- Enabled by default. Config: `Kafka:DeadLetterEnabled` (default `true`), `Kafka:DeadLetterTopicSuffix` (default `.dlq`). Env: `Kafka__DeadLetterEnabled`, `Kafka__DeadLetterTopicSuffix`.
- Record: the same key (may be null for legacy messages), the same value (may be null for tombstones) and the original headers. Metadata headers below are added; any metadata header already on the original message is replaced, so a replayed record never carries two copies.

| Header | Value |
|---|---|
| `dlq-original-topic` | Topic consumed from, UTF-8 |
| `dlq-original-partition` | Partition, decimal |
| `dlq-original-offset` | Offset, decimal |
| `dlq-exception-type` | Full type name of the last exception thrown by the handler (no exception message: it can contain personal data). `MessageOutcome.Retry` when the handler returned `Retry` without throwing |
| `dlq-attempt-count` | Failed handler invocations for this message in this consumer, decimal (normally 5) |
| `dlq-failed-at-utc` | UTC time of dead-lettering, ISO 8601 round-trip (`O`), e.g. `2026-11-15T10:30:00.0000000Z` |

Producer settings are the shared ones (`acks=all`, idempotence, 10s message timeout), the same as `KafkaProducerService`.

Delivery to the dead-letter topic is at-least-once: if the offset commit fails after a successful dead-letter publish, the message is dead-lettered again on redelivery. DLQ readers must dedupe on `(dlq-original-topic, dlq-original-partition, dlq-original-offset)`.

Operational requirements:
- The dead-letter topic must exist before deploy (or be auto-created). If a publish fails, the partition pauses until restart, so a missing topic stalls that partition rather than dropping messages.
- Under SASL, the consumer principal needs WRITE on `<topic>.dlq` in addition to READ on the source topic.
- Retry attempts are counted in consumer memory. A rebalance restarts the count; the message is still never committed without being handled or dead-lettered.
- Nothing consumes the dead-letter topics yet. Replay is a manual or future tool; until then, DLQ volume needs monitoring.

Config — dev (local broker, no auth), only bootstrap is set:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

Config — prod (SASL + TLS, fail-fast on half config). Username set without password, unknown protocol/mechanism, or missing CA file throws at startup:

```bash
KAFKA_BOOTSTRAP_SERVERS=broker:9092
Kafka__SaslUsername=svc
Kafka__SaslPassword=<secret>          # required with username
Kafka__SecurityProtocol=SaslSsl       # auto when username set; explicit override ok
Kafka__SaslMechanism=ScramSha256      # Plain (default) | ScramSha256 | ScramSha512
Kafka__SslCaLocation=/certs/ca.crt    # optional; must exist when set
Kafka__DeadLetterEnabled=true         # default true
Kafka__DeadLetterTopicSuffix=.dlq     # default .dlq
```

## Release

Released versions are published to GitHub Packages by `.github/workflows/release.yml` when a `vX.Y.Z` tag is pushed:

1. Merge a PR to `main` that bumps `<Version>` in `src/SharedKernel/SharedKernel.csproj` and adds a `## [X.Y.Z]` entry to `CHANGELOG.md`.
2. `git tag vX.Y.Z` on that commit and `git push origin vX.Y.Z`.
3. The workflow checks that the tag equals `<Version>` and that CHANGELOG has the entry, then runs build, tests and `dotnet pack`, and pushes the package with the built-in `GITHUB_TOKEN`.

GitHub Packages never overwrites a version. If `X.Y.Z` is already on the feed, the publish job fails with a message: the release needs a new version.

## Consume

Services take the package from GitHub Packages (`https://nuget.pkg.github.com/dut-pbl6-2026/index.json`). Add the source once per machine or CI runner, with a personal access token that has `read:packages` (never commit the token):

```bash
dotnet nuget add source https://nuget.pkg.github.com/dut-pbl6-2026/index.json \
  --name github-dut-pbl6-2026 \
  --username <github-username> \
  --password <token-with-read:packages> \
  --store-password-in-clear-text
```

In CI, pass the token from a secret instead of a literal. Then reference `JobPlatform.SharedKernel` with `PackageReference` as usual.

Local development against unreleased changes still uses the `local-feed` flow below.


`JobPlatform.SharedKernel 0.2.0` via `local-feed` + `nuget.config` in `job-platform-auth-svc` (`PackageReference` not `ProjectReference`). Services still on `0.1.0` keep working — bump to `0.2.0` only when adopting Kafka events (`PBL6-5`). For local dev:

```bash
mise run pack
cp artifacts/*.nupkg ../job-platform-auth-svc/local-feed/
```

When bumping SharedKernel, update `<Version>` in
`src/SharedKernel/SharedKernel.csproj`, then run:

```powershell
mise run verify
mise run syncLocalFeeds
```

`syncLocalFeeds` packs SharedKernel, copies the package to the local feeds
of auth-svc and gateway, and updates their exact PackageReference versions
in Auth.Core, Auth.Infrastructure, and Gateway.Api.

Requires sibling clones of shared, auth-svc, and gateway, plus PowerShell
and mise.

After synchronization, run restore, build, and tests in both consumers.
Commit the new packages and updated project files in their respective repos.
```

## Troubleshooting

- `dotnet: command not found` → `mise trust` not run → `mise exec -- dotnet --version`
- `NU1903` vulnerability → bump `System.Security.Cryptography.Xml 10.0.11`

`feature/* → main` (see `job-platform-docs/.github/git-strategy.md`).

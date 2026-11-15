# Changelog

All notable changes to `JobPlatform.SharedKernel` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-11-15

### Removed

- **Breaking:** `JwtOptions.Secret` is removed. Tokens are RS256 and verified from auth-svc's JWKS, so no service holds a shared signing secret. Consumers must not set or read `Jwt:Secret`.

## [0.3.1] - 2026-11-15

### Fixed

- Kafka consumer: a `Retry` outcome or a thrown handler exception now seeks back to the failed offset and redelivers after backoff (1s, 2s, 4s, 8s). Previously the next commit could skip the failed message (#29).
- Kafka consumer: null or blank message values (tombstones) are skipped instead of throwing `ArgumentNullException` (#29).

### Added

- Kafka consumer: exhausted messages (5 failed handler invocations) are published to the dead-letter topic `<topic>.dlq`, with the original key, value and headers plus `dlq-*` metadata headers. The offset is committed only after the publish is acknowledged (#30).
- Dead-letter configuration: `Kafka:DeadLetterEnabled` (default `true`) and `Kafka:DeadLetterTopicSuffix` (default `.dlq`). See the README "Dead-letter topic" section.
- CI runs `dotnet test` on push and pull request to `main` (#29).

## [0.3.0] - 2026-11-05

### Added

- Optional `CreatedAt` on `JobCreatedEvent` and `JobUpdatedEvent`. Additive; `null` from older producers.

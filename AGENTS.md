# AGENTS — job-platform-shared

> SharedKernel NuGet. SRS: `job-platform-docs/docs/master-plan.md:132,150`, `docs/srs/en/{8-system-architecture:8.7,3-must-have-fr:3.13,6-nfr:MAINT}`. Git: `job-platform-docs/.github/git-strategy.md`.

## Mise activation

Activate `mise` for bare `dotnet`/`infisical` without `mise exec`:

| Shell | Add to config file | Activate |
|-------|--------------------|----------|
| `bash` | `~/.bashrc` or `~/.bash_profile` | `eval "$(mise activate bash)"` |
| `zsh` | `~/.zshrc` | `eval "$(mise activate zsh)"` |
| `fish` | `~/.config/fish/config.fish` | `mise activate fish \| source` |
| `PowerShell` | `$PROFILE` | `mise activate pwsh \| Out-String \| Invoke-Expression` |

Agent uses `mise exec -- dotnet ...` / `mise exec -- infisical ...` due to non-interactive shell without `mise activate`; humans just use `dotnet` / `infisical` after `mise install`.

## Scope

`PBL6-11` shared library — `PackageId JobPlatform.SharedKernel 0.3.1` (`net10.0`) for all svcs+gateway. Owner all TMs. MUST infra dependency per `3.13`. `0.2.0` adds Kafka event contracts + plumbing (`PBL6-5`, `src/SharedKernel/{Events,Kafka}`); consumers on `0.1.0` bump `PackageReference` when they adopt events. `0.3.0` adds optional `CreatedAt` to `JobCreatedEvent`/`JobUpdatedEvent` (additive, null from older producers): job-svc and search-svc use it; other services can stay on `0.2.0`.

## Architecture (SRS 8.7 multirepo)

- **Consumers:** `auth-svc` + `job-svc` + `search-svc` + `app-svc` + `profile-svc` + `notif-svc` + `gateway` via `PackageReference` only (never `ProjectReference` per `master-plan.md:132`), `nuget.config` `local-feed` in `auth-svc` for local dev; released versions come from GitHub Packages (see Release).
- **Contracts:** DDD building blocks + Kafka event contracts/plumbing (`PBL6-5`) — no other infra. Event schema dual-read for `job.created|updated|deleted|application.submitted|status_changed` (`8-system-architecture.md:8.5`): `Events/{JobEvents,ApplicationEvents,EventEnvelope}.cs` (payloads + envelope, no PII per `SEC-05`), `Kafka/{KafkaOptions,KafkaProducerService,KafkaConsumerService}.cs` (transport only; topic/group stay per-service).
- **Versioning:** SemVer `0.1.0` (`<Version>` in `SharedKernel.csproj`), breaking change = major bump + `Pact` contract test block in service CI (`3-must-have-fr.md:3.13`), Dependabot + `repository_dispatch` propagation.

## DDD building blocks (2026 best practice, NFR `MAINT-01`)

```csharp
Entity          : Id=Guid.NewGuid(), CreatedAt/UpdatedAt=UtcNow, Touch()
ValueObject     : IEquatable<ValueObject>, GetEqualityComponents() + SequenceEqual + HashCode.Combine + ==/!=
Result / Result<T>: IsSuccess/Error, Success/Failure(string) — use for domain failures, not exceptions
JwtOptions      : SectionName="Jwt", Issuer=Audience="job-platform", ExpiresMinutes=60 (no secret: RS256 keys come from auth-svc JWKS)
```

- Namespace `SharedKernel`, `Nullable enable` + `ImplicitUsings`, file-scoped namespace, record-like immutability where sensible (`src/SharedKernel: Entity.cs/ValueObject.cs/Result.cs/JwtOptions.cs`).
- `GenerateDocumentationFile true` — XML docs required on every public type/method.

## 2026 best practice (NFR `MAINT-02/03`)

- `dotnet 10.0.100` `net10.0`, `dotnet build --warnaserror` + `dotnet format --verify-no-changes` (mise `build/format`), keep `ImplicitUsings` + `Nullable`.
- Coverage `>70%` via consumers, plus direct xUnit suite `tests/SharedKernel.Tests` for Events/Kafka (`mise run test`), `OpenAPI` via consumers, `Bump System.Security.Cryptography.Xml 10.0.11` for `NU1903`.
- Keep `SharedKernel.csproj` minimal — no `EF`/`Npgsql`/`BCrypt`, no `appsettings.json`. Only allowed extra refs are the Kafka set (`Confluent.Kafka 2.*` + `Microsoft.Extensions.{Hosting,Logging,Options}.Abstractions 10.0.0` for the producer/consumer base).
- Kafka rules for producers/consumers: partition key is mandatory (`JobId`/`ApplicationId`, plan `2.2.1`), payloads carry no PII (`SEC-05`), consumers commit offsets only after successful handling (at-least-once, handlers must be idempotent). After the retry budget (5 attempts) the message goes to `<topic>.dlq` with the original key, value and headers plus `dlq-*` metadata headers; the partition then continues. Topic naming, config and the header table: README "Dead-letter topic". Consumer services must create `<topic>.dlq` (or enable auto-create) before deploy.

## Workflow

```bash
mise trust && mise install
mise run build && mise run format
mise run pack   # → ./artifacts/JobPlatform.SharedKernel.0.2.0.nupkg
mise run verify # 1 nupkg
cp artifacts/*.nupkg ../job-platform-auth-svc/local-feed/
```

`feature/* → main` (e.g., `feature/PBL6-12-shared-kernel`), PR must update `CHANGELOG` on version bump.

## Release

Packages are published to the GitHub Packages NuGet feed `https://nuget.pkg.github.com/dut-pbl6-2026/index.json` by `.github/workflows/release.yml`. Only a tag pushed to a commit matching `v*.*.*` starts it; a plain push to `main` does not publish.

1. PR to `main`: bump `<Version>` in `src/SharedKernel/SharedKernel.csproj` and add a `## [X.Y.Z] - YYYY-MM-DD` entry to `CHANGELOG.md`. Merge it.
2. Tag the merged commit: `git tag vX.Y.Z && git push origin vX.Y.Z`.
3. The workflow: the `test` job fails unless the tag equals `<Version>` and CHANGELOG has an entry for it, then runs build (`--warnaserror`) and tests. The `publish` job (`packages: write` only there) packs and pushes with the built-in `GITHUB_TOKEN`.
4. Versions are immutable on the feed. If `X.Y.Z` already exists, the push fails with a message saying so; there is no overwrite and no skip. Fix a bad release by bumping to a new version, never by re-tagging.

Consumers: add the GitHub Packages source with a token that has `read:packages` (see README "Consume"). Do not commit the token.

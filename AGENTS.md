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

`PBL6-11` shared library — `PackageId JobPlatform.SharedKernel 0.1.0` (`net10.0`) for all svcs+gateway. Owner all TMs. MUST infra dependency per `3.13`.

## Architecture (SRS 8.7 multirepo)

- **Consumers:** `auth-svc` + `job-svc` + `search-svc` + `app-svc` + `profile-svc` + `notif-svc` + `gateway` via `PackageReference` only (never `ProjectReference` per `master-plan.md:132`), `nuget.config` `local-feed` in `auth-svc` for local dev.
- **Contracts:** DDD building blocks only — no infra. Event schema dual-read for `job.created|updated|deleted|application.submitted|updated` (`8-system-architecture.md:8.5`).
- **Versioning:** SemVer `0.1.0` (`<Version>` in `SharedKernel.csproj`), breaking change = major bump + `Pact` contract test block in service CI (`3-must-have-fr.md:3.13`), Dependabot + `repository_dispatch` propagation.

## DDD building blocks (2026 best practice, NFR `MAINT-01`)

```csharp
Entity          : Id=Guid.NewGuid(), CreatedAt/UpdatedAt=UtcNow, Touch()
ValueObject     : IEquatable<ValueObject>, GetEqualityComponents() + SequenceEqual + HashCode.Combine + ==/!=
Result / Result<T>: IsSuccess/Error, Success/Failure(string) — use for domain failures, not exceptions
JwtOptions      : SectionName="Jwt", Secret≥32, Issuer=Audience="job-platform", ExpiresMinutes=60
```

- Namespace `SharedKernel`, `Nullable enable` + `ImplicitUsings`, file-scoped namespace, record-like immutability where sensible (`src/SharedKernel: Entity.cs/ValueObject.cs/Result.cs/JwtOptions.cs`).
- `GenerateDocumentationFile true` — XML docs required on every public type/method.

## 2026 best practice (NFR `MAINT-02/03`)

- `dotnet 10.0.100` `net10.0`, `dotnet build --warnaserror` + `dotnet format --verify-no-changes` (mise `build/format`), keep `ImplicitUsings` + `Nullable`.
- Coverage `>70%` via consumers, `OpenAPI` via consumers, `Bump System.Security.Cryptography.Xml 10.0.11` for `NU1903`.
- Keep `SharedKernel.csproj` minimal — no `EF`/`Npgsql`/`BCrypt`, no `appsettings.json`.

## Workflow

```bash
mise trust && mise install
mise run build && mise run format
mise run pack   # → ./artifacts/JobPlatform.SharedKernel.0.1.0.nupkg
mise run verify # 1 nupkg
cp artifacts/*.nupkg ../job-platform-auth-svc/local-feed/
```

`feature/* → main` (e.g., `feature/PBL6-12-shared-kernel`), PR must update `CHANGELOG` on version bump, tag `v0.1.0` on `main` triggers NuGet push to GHCR/Feed.

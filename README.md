# job-platform-shared

.NET Class Library SharedKernel — **Vietnam Job Platform** (`pbl6`) under [`dut-pbl6-2026`](https://github.com/dut-pbl6-2026). `PackageId JobPlatform.SharedKernel 0.1.0`.

- **Tech:** `net10.0` `dotnet 10.0.100` via `mise`
- **Branch flow:** `feature/* → main` (see `job-platform-docs/.github/git-strategy.md`)
- **Jira:** `PBL6` `skid.atlassian.net` `master-plan.md:150`

## Prerequisites

- `mise` + `dotnet 10.0.100` `mise trust && mise install`
- `git` `gh`

## Clone

```bash
gh repo clone dut-pbl6-2026/job-platform-shared
cd job-platform-shared
```

## Setup

```bash
mise trust
mise install
mise exec -- dotnet --version  # 10.0.100
```

No `env` needed for this repo (library). For full stack `env` see `job-platform-infra/envs/.env.dev.example` + `scripts/sync-env.sh dev`.

## Build

```bash
mise exec -- dotnet build SharedKernel.sln --warnaserror
mise exec -- dotnet format --verify-no-changes SharedKernel.sln
mise exec -- dotnet pack src/SharedKernel/SharedKernel.csproj -c Release -o ./artifacts  # creates JobPlatform.SharedKernel.0.1.0.nupkg
ls artifacts/
```

- `src/SharedKernel` `Result<T>` `Entity` `ValueObject` `JwtOptions` `ValueObject IEquatable` + `operator==`.
- `GenerateDocumentationFile` true — XML docs required.

## Consume

Published as `JobPlatform.SharedKernel 0.1.0` via `local-feed` + `nuget.config` in `job-platform-auth-svc` (`PackageReference` not `ProjectReference` per `master-plan.md:132`). For local dev `dotnet pack` then `cp artifacts/*.nupkg ../job-platform-auth-svc/local-feed/`.

## Troubleshooting

- `dotnet: command not found` → `mise trust` not run or `eval "$(mise activate bash)"` missing.
- `NU1903` vulnerability → bump `System.Security.Cryptography.Xml 10.0.11`.

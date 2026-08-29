# job-platform-shared

.NET Class Library SharedKernel — **Vietnam Job Platform** (`pbl6`) under [`dut-pbl6-2026`](https://github.com/dut-pbl6-2026). `PackageId JobPlatform.SharedKernel 0.1.0`.

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
mise run format  # dotnet format --verify-no-changes
mise run pack    # dotnet pack -o ./artifacts
mise run verify  # check artifacts nupkg
```

- `src/SharedKernel` `Result<T>` `Entity` `ValueObject` `JwtOptions`.
- `GenerateDocumentationFile` true — XML docs required.

## Consume

`JobPlatform.SharedKernel 0.1.0` via `local-feed` + `nuget.config` in `job-platform-auth-svc` (`PackageReference` not `ProjectReference`). For local dev:

```bash
mise run pack
cp artifacts/*.nupkg ../job-platform-auth-svc/local-feed/
```

## Troubleshooting

- `dotnet: command not found` → `mise trust` not run → `mise exec -- dotnet --version`
- `NU1903` vulnerability → bump `System.Security.Cryptography.Xml 10.0.11`

`feature/* → main` (see `job-platform-docs/.github/git-strategy.md`).

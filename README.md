# job-platform-shared

.NET Class Library SharedKernel — **Vietnam Job Platform** (`pbl6`) under [`dut-pbl6-2026`](https://github.com/dut-pbl6-2026). `PackageId JobPlatform.SharedKernel 0.1.0`.

- **Tech:** `net10.0` `dotnet 10.0.100` via `mise`
- **Branch flow:** `feature/* → main` (see `job-platform-docs/.github/git-strategy.md`)
- **Jira:** `PBL6` `skid.atlassian.net` `master-plan.md:150`

## Prerequisites

- `mise` https://mise.jdx.dev + `dotnet 10.0.100` — `mise trust && mise install && mise exec -- dotnet --version # 10.0.100`
- Activate (optional, for bare `dotnet` without `mise exec`):

  | Shell | Add to config file | Activate |
  |-------|--------------------|----------|
  | `bash` | `~/.bashrc` or `~/.bash_profile` | `eval "$(mise activate bash)"` |
  | `zsh` | `~/.zshrc` | `eval "$(mise activate zsh)"` |
  | `fish` | `~/.config/fish/config.fish` | `mise activate fish \| source` |
  | `PowerShell` | `$PROFILE` | `mise activate pwsh \| Out-String \| Invoke-Expression` |
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
dotnet --version  # 10.0.100
```

> Note: agent uses `mise exec -- dotnet ...` due to non-interactive shell without `mise activate`; humans just use `dotnet`.

No `env` needed for this repo (library). For full stack `env` see `job-platform-infra/envs/.env.dev.example` + `scripts/sync-env.sh dev`.

## Build

```bash
mise run build   # dotnet build --warnaserror
mise run format  # dotnet format --verify-no-changes
mise run pack    # dotnet pack -o ./artifacts
ls artifacts/
```

- `src/SharedKernel` `Result<T>` `Entity` `ValueObject` `JwtOptions` `ValueObject IEquatable` + `operator==`.
- `GenerateDocumentationFile` true — XML docs required.

## Consume

Published as `JobPlatform.SharedKernel 0.1.0` via `local-feed` + `nuget.config` in `job-platform-auth-svc` (`PackageReference` not `ProjectReference` per `master-plan.md:132`). For local dev `dotnet pack` then `cp artifacts/*.nupkg ../job-platform-auth-svc/local-feed/`.

> Note: agent uses `mise exec -- dotnet ...`; humans use `dotnet` directly after `mise install`.

## Troubleshooting

- `dotnet: command not found` → run `mise trust && mise install`; if still missing ensure `mise activate` configured for your shell (`bash`/`zsh`/`fish`/`pwsh` — see Prerequisites) or use `mise exec -- dotnet --version`.
- `NU1903` vulnerability → bump `System.Security.Cryptography.Xml 10.0.11`.

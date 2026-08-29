## Summary

<!-- What + why (1-2 lines). Link Jira: PBL6-11/12 etc. Branch: feature/* → main -->

## Changes

- [ ] `mise run build` — `dotnet build SharedKernel.sln --warnaserror`
- [ ] `mise run format` — `dotnet format --verify-no-changes`
- [ ] `mise run pack` / `verify` if touching `src/SharedKernel`
- [ ] Docs updated (`README.md` / `AGENTS.md`)

## How to verify

```bash
mise trust && mise install
mise run build
mise run format
mise run pack
mise run verify  # 1 nupkg in artifacts
ls artifacts/
```

## Checklist

- [ ] `GenerateDocumentationFile` XML docs present (`src/SharedKernel`)
- [ ] `mise run build` passes with `--warnaserror`
- [ ] No `.env` committed (`.gitignore`) — library has no env
- [ ] `artifacts/*.nupkg` not committed (except via release flow)

Closes #

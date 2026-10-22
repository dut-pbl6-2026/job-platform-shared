param(
    [string]$AuthRepo = (Join-Path $PSScriptRoot "../../job-platform-auth-svc"),
    [string]$GatewayRepo = (Join-Path $PSScriptRoot "../../job-platform-gateway")
)

$ErrorActionPreference = "Stop"

$sharedRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$sharedProject = Join-Path $sharedRoot "src/SharedKernel/SharedKernel.csproj"

[xml]$projectXml = Get-Content -LiteralPath $sharedProject -Raw
$version = [string]$projectXml.Project.PropertyGroup.Version

if ([string]::IsNullOrWhiteSpace($version)) {
    throw "SharedKernel.csproj must declare Version."
}

$targets = @(
    @{
        Root = (Resolve-Path -LiteralPath $AuthRepo).Path
        ProjectPath = "src/Auth.Core/Auth.Core.csproj"
    },
    @{
        Root = (Resolve-Path -LiteralPath $AuthRepo).Path
        ProjectPath = "src/Auth.Infrastructure/Auth.Infrastructure.csproj"
    },
    @{
        Root = (Resolve-Path -LiteralPath $GatewayRepo).Path
        ProjectPath = "src/Gateway.Api/Gateway.Api.csproj"
    }
)

$pattern = '(<PackageReference\s+Include="JobPlatform\.SharedKernel"\s+Version=")[^"]+(")'

# Check both consumers before making changes.
foreach ($target in $targets) {
    $target.Project = Join-Path $target.Root $target.ProjectPath
    $content = Get-Content -LiteralPath $target.Project -Raw

    if ([regex]::Matches($content, $pattern).Count -ne 1) {
        throw "Expected one SharedKernel PackageReference in $($target.Project)"
    }

    $config = Join-Path $target.Root "nuget.config"

    if (-not (Test-Path -LiteralPath $config)) {
        throw "Missing nuget.config in $($target.Root)"
    }
}

# Use a separate output directory to avoid copying stale packages.
$output = Join-Path $sharedRoot (
    "artifacts/feed-sync-" + [Guid]::NewGuid().ToString("N")
)

Push-Location $sharedRoot
try {
    & mise exec -- dotnet pack $sharedProject -c Release -o $output

    if ($LASTEXITCODE -ne 0) {
        throw "SharedKernel pack failed."
    }
}
finally {
    Pop-Location
}

$package = Join-Path $output "JobPlatform.SharedKernel.$version.nupkg"

if (-not (Test-Path -LiteralPath $package)) {
    throw "Expected package not found: $package"
}

foreach ($target in $targets) {
    $feed = Join-Path $target.Root "local-feed"
    New-Item -ItemType Directory -Path $feed -Force | Out-Null

    Copy-Item -LiteralPath $package -Destination $feed -Force

    $content = Get-Content -LiteralPath $target.Project -Raw

    $updated = [regex]::Replace(
        $content,
        $pattern,
        {
            param($match)
            $match.Groups[1].Value +
            "[$version]" +
            $match.Groups[2].Value
        }
    )
    $updated = $updated.Replace("`r`n", "`n")

    [System.IO.File]::WriteAllText(
        $target.Project,
        $updated,
        [System.Text.UTF8Encoding]::new($false)
    )

    Write-Host "Updated $($target.Root) to SharedKernel [$version]"
}
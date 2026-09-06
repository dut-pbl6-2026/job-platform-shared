# Backlog — SharedKernel debt (PBL6-11)

> Sprint 1 đủ dùng (Auth chỉ cần `Entity` + `Result` + `JwtOptions`). Các mục dưới là nợ kỹ thuật, fix ở Sprint 2+ khi có consumer đầu tiên. Format theo `job-platform-docs/docs/backlog.md`: File + Log + Fix.

## 1) Không có test project riêng
- File: `src/SharedKernel/*.cs` (`.sln` chỉ 1 project), `.github/workflows/ci.yml` (không chạy test)
- Log: `coverage >70% via consumers` — regress ở `ValueObject.Equals/GetHashCode`, `Result<T>` không bị chặn ở repo này.
- Fix (Sprint 2): thêm `tests/SharedKernel.Tests` (xUnit), CI chạy `dotnet test`, target >70%.

## 2) Result chỉ có string Error
- File: `src/SharedKernel/Result.cs:9,21`
- Log: thiếu `Code`, multi-error validation, `Map/Match/Combine`; `Value` là `T?` awkward với value-type; `new Failure` che `base.Failure` dễ nhầm.
- Fix (Sprint 2, khi job/app-svc cần validation): thêm `Error` type + `Map/Match`, giữ compat 0.x.

## 3) Entity thiếu DomainEvents
- File: `src/SharedKernel/Entity.cs`
- Log: SRS 8.5 cần `job.created|updated|deleted|application.submitted|updated` nhưng base chưa có `AddDomainEvent/Clear`.
- Fix (Sprint 2, cùng job-svc): thêm `List<IDomainEvent>` + `IDomainEvent` base, không breaking.

## 4) DateTime.UtcNow trực tiếp, khó test thời gian
- File: `src/SharedKernel/Entity.cs:9-11`
- Log: `CreatedAt/UpdatedAt=UtcNow` + `Touch()` gọi clock tĩnh → không mock được thời gian trong test.
- Fix (Sprint 2): chấp nhận hoặc abstract qua `TimeProvider`; effort nhỏ.

## 5) JwtOptions chưa validate
- File: `src/SharedKernel/JwtOptions.cs:9,14`
- Log: `Secret=""` default, `>=32 chars` chỉ nằm trong comment; `ExpiresMinutes` không check `>0`.
- Fix (Sprint 1.5/2): thêm `IValidateOptions<JwtOptions>` phía consumer hoặc DataAnnotations, không đổi shape.

## 6) Thiếu blocks sẽ bị duplicate
- File: (chưa có) `PagedResult/Pagination`, `Guard`, `IntegrationEvent envelope`
- Log: sang Sprint 2 (search/pagination, Kafka events) mỗi svc sẽ tự chế nếu kernel không có → mất tác dụng shared.
- Fix (Sprint 2): bổ sung từng cái khi có consumer đầu tiên, bump minor `0.2.0`.

## 7) Release còn thủ công
- File: `src/SharedKernel/SharedKernel.csproj:8`, `mise.toml` (`verify`), `.github/workflows/ci.yml`
- Log: `Version` hardcode, `verify` chỉ đếm nupkg, chưa push GHCR khi tag `v*`, chưa enforce CHANGELOG/Pact.
- Fix (Sprint 2): thêm workflow `tag v* → pack → push GHCR`, consumer `repository_dispatch`.

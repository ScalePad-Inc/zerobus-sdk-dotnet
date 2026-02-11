# Copilot Instructions — Zerobus .NET SDK

## Project Overview

This is the **Databricks Zerobus .NET SDK** (`Databricks.Zerobus`), a high-performance .NET library for streaming data ingestion into Databricks Delta tables via the Zerobus gRPC service. The SDK wraps a Rust core library (`databricks-zerobus-ingest-sdk`) exposed through a C FFI layer (`zerobus-ffi`) using P/Invoke.

### Architecture

```
.NET SDK (Databricks.Zerobus)
    ↓ P/Invoke (DllImport)
Rust FFI crate (zerobus-ffi / libzerobus_ffi)
    ↓
Rust core (databricks-zerobus-ingest-sdk)
    ↓ gRPC
Zerobus Service
```

## Repository Structure

| Path                              | Description                                                                                   |
| --------------------------------- | --------------------------------------------------------------------------------------------- |
| `src/Zerobus/`                    | Main SDK library (namespace: `Databricks.Zerobus`)                                            |
| `src/Zerobus/Native/`             | Internal P/Invoke layer — `NativeBindings.cs`, `NativeInterop.cs`, `HeadersProviderBridge.cs` |
| `tests/Zerobus.Tests/`            | Unit tests (NUnit, no native library needed)                                                  |
| `tests/Zerobus.IntegrationTests/` | Integration tests (NUnit + mock gRPC server, requires native library)                         |
| `examples/`                       | Example apps: `JsonSingle/`, `JsonBatch/`, `ProtoSingle/`                                     |
| `zerobus-ffi/`                    | Rust FFI crate that produces the native shared library                                        |
| `build_native.sh`                 | Script to build the Rust FFI shared library for the current platform                          |

## Tech Stack & Frameworks

- **Languages:** C# (.NET 8 / .NET 10), Rust
- **Build:** MSBuild via `dotnet` CLI; Cargo for the Rust crate
- **Test framework:** NUnit 4 with NUnit3TestAdapter
- **Mocking:** NSubstitute (unit tests), custom mock gRPC server (integration tests)
- **gRPC/Protobuf:** Grpc.AspNetCore, Grpc.Tools, Google.Protobuf (integration tests only)
- **Nullable reference types:** Enabled globally
- **Implicit usings:** Enabled globally
- **Warnings as errors:** Enabled globally (`TreatWarningsAsErrors`)
- **Language version:** `latest`
- **Unsafe code:** Allowed in the main library (required for P/Invoke with pointers)

## Coding Conventions

### C# Style

- Use `sealed` on classes that are not designed for inheritance.
- Use C# record types for immutable data (`record`, `record class`). See `StreamConfigurationOptions`, `TableProperties`.
- Use `with` expressions for record mutation (not builder pattern).
- Use `IDisposable` / `using` for resource management. Both `ZerobusSdk` and `ZerobusStream` implement `IDisposable`.
- Use `ArgumentNullException.ThrowIfNull()` and `ObjectDisposedException.ThrowIf()` guard clauses.
- Prefer expression-bodied members for single-expression methods/properties.
- Use XML doc comments (`///`) on all public API members with `<summary>`, `<param>`, `<returns>`, `<exception>`, `<example>`, and `<remarks>` tags as appropriate.
- Internal types go in the `Databricks.Zerobus.Native` namespace under the `Native/` folder.
- Public API types go directly in the `Databricks.Zerobus` namespace.
- Use `InternalsVisibleTo` (in `Properties/AssemblyInfo.cs`) for test access to internals.
- Use collection expressions (`[...]`) over explicit array/list constructors where possible.
- Use raw string literals (`"""..."""`) for embedded JSON.
- Use UTF-8 string literals (`"..."u8.ToArray()`) for byte array test data.
- File-scoped namespaces (no braces around the entire file).
- One type per file, file name matches type name.

### Naming

- PascalCase for public members, types, properties, methods.
- `_camelCase` with underscore prefix for private fields.
- Interfaces prefixed with `I` (e.g., `IHeadersProvider`).
- Exceptions suffixed with `Exception` (e.g., `ZerobusException`).
- Test classes suffixed with `Tests` (e.g., `ZerobusSdkTests`).
- Test methods: `MethodOrScenario_Condition_ExpectedResult` (e.g., `Constructor_NullEndpoint_ThrowsArgumentNullException`).

### Rust Style (zerobus-ffi crate)

- Use `pub(crate)` for internal helpers, `#[no_mangle] pub extern "C"` for FFI exports.
- FFI function names follow the pattern: `zerobus_<entity>_<action>` (e.g., `zerobus_sdk_new`, `zerobus_stream_flush`).
- Use `#[repr(C)]` for all structs crossing the FFI boundary.
- All FFI functions write results to a `CResult` out-parameter; never panic across FFI boundary.
- Opaque types use zero-sized arrays (`[u8; 0]`) and are only handled via pointer.

## Testing Guidelines

- **Unit tests** (`tests/Zerobus.Tests/`): Test managed code in isolation. Do NOT require the native library. Use NSubstitute for mocking interfaces. Run with `dotnet test tests/Zerobus.Tests`.
- **Integration tests** (`tests/Zerobus.IntegrationTests/`): Spin up a per-test mock gRPC server (`MockZerobusServer`) and exercise the full SDK through the native FFI layer. Require the Rust toolchain. Tests run in parallel (`[Parallelizable(ParallelScope.Children)]`). Run with `dotnet test tests/Zerobus.IntegrationTests`.
- Use `[TestFixture]` on test classes and `[Test]` on test methods (NUnit 4).
- Use NUnit constraint model assertions: `Assert.That(value, Is.EqualTo(...))`, `Assert.Throws<T>(...)`.
- Run all tests: `dotnet test`.

## Build Notes

- `dotnet build` automatically invokes `build_native.sh` to compile the Rust FFI shared library (skip with `-p:SkipNativeBuild=true`).
- The native library is placed under `src/Zerobus/runtimes/<RID>/native/` following .NET runtime identifier conventions.
- Multi-targeting: `net8.0` and `net10.0`. The native build only runs for the first TFM to avoid duplicate cargo builds.
- The `runtimes/` directory is gitignored — native libraries are built from source or provided by CI.

## Error Handling

- All SDK errors throw `ZerobusException` which has an `IsRetryable` property.
- Native errors from Rust arrive via `CResult` structs and are converted to `ZerobusException` in `NativeInterop.ThrowIfFailed()`.
- The `Message` property is formatted with a `ZerobusException:` or `ZerobusException (retryable):` prefix; the `RawMessage` property contains the original error string.

## Key Patterns

- **Resource lifecycle:** `ZerobusSdk` and `ZerobusStream` are `IDisposable` + have a finalizer. They wrap raw `IntPtr` handles to native Rust objects.
- **Headers provider bridge:** `IHeadersProvider` → `HeadersProviderBridge` → native function pointer callback. A `GCHandle` pins the bridge for the stream's lifetime.
- **Config conversion:** Managed `StreamConfigurationOptions` → `CStreamConfigurationOptions` (native struct) via `NativeInterop.ConvertConfig()`.
- **Thread safety:** Both `ZerobusSdk` and `ZerobusStream` are thread-safe per the Rust core guarantees.

## Commit Convention

This project uses **Conventional Commits**. All commit messages must follow this format:

```
<type>(<scope>): <short summary>

[optional body]

[optional footer(s)]
```

### Types

| Type       | Description                                                           |
| ---------- | --------------------------------------------------------------------- |
| `feat`     | A new feature                                                         |
| `fix`      | A bug fix                                                             |
| `docs`     | Documentation-only changes                                            |
| `style`    | Code style changes (formatting, whitespace, etc. — no logic change)   |
| `refactor` | Code change that neither fixes a bug nor adds a feature               |
| `perf`     | Performance improvement                                               |
| `test`     | Adding or updating tests                                              |
| `build`    | Changes to the build system or dependencies (MSBuild, Cargo, scripts) |
| `ci`       | CI/CD configuration changes                                           |
| `chore`    | Maintenance tasks (tooling, config, etc.)                             |

### Scopes

Use one of the following scopes (or omit for cross-cutting changes):

- `sdk` — Main SDK library (`src/Zerobus/`)
- `ffi` — Rust FFI crate (`zerobus-ffi/`)
- `native` — P/Invoke / native interop layer (`src/Zerobus/Native/`)
- `tests` — Unit or integration tests
- `examples` — Example applications
- `docs` — Documentation (README, comments)
- `build` — Build scripts, MSBuild props, Cargo config

### Rules

- Use imperative mood in the summary: "add feature" not "added feature" or "adds feature".
- Do not end the summary with a period.
- Keep the summary line under 72 characters.
- Use the body to explain _what_ and _why_, not _how_.
- Reference issues in the footer: `Closes #123` or `Refs #456`.
- Add `BREAKING CHANGE:` in the footer (or `!` after the type/scope) for breaking changes.

### Examples

```
feat(sdk): add StreamPausedMaxWaitTimeMs configuration option

fix(native): prevent double-free in HeadersProviderBridge callback

test(tests): add integration test for batch ingest after close

build(ffi): bump databricks-zerobus-ingest-sdk to 0.4.0

docs: update README with concurrent ingestion examples

refactor(sdk)!: rename IngestData to IngestRecord

BREAKING CHANGE: IngestData method has been renamed to IngestRecord.
```

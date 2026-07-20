# AGENTS.md

## Overview
R package `BD` — internal wrapper around DBI/RSQLite for database operations. SQLite-only; ACCESS support was removed.

## Commands
- **Document & NAMESPACE**: `devtools::document()` (roxygen2, NAMESPACE is auto-generated — do not edit by hand)
- **Check**: `devtools::check()`
- **Test**: `devtools::test()` (103 tests across 4 files)
- **Coverage**: `covr::package_coverage()` (100% overall)
- **Install**: `devtools::install()` or `R CMD INSTALL .`

## Architecture
- `R/BD_env.R` — internal environment (`BD_env`) storing path aliases across a session
- `R/path.R` — path resolution and aliases: `path()`, `set_path()`, `clear_paths()`, `list_paths()`, `BD_type()`
- `R/connection.R` — `connection()` (SQLite via DBI/RSQLite)
- `R/glue.R` — SQL interpolation: `glue()`, `glueData()`
- `R/query.R` — query helpers: `GetQuery()`, `GetQueryData()`
- `R/execute.R` — DML execution: `Execute()`, `ExecuteData()`
- `R/table.R` — table CRUD: `ReadTable()`, `WriteTable()`, `CreateTable()`, `AppendTable()`, `OverwriteTable()`, `RemoveTable()`, `ExistsTable()`, `ListTables()`

## Database path resolution
`path(.which, .path)` resolves DB paths via:
1. Session-stored aliases (set via `set_path(.path, .which)`)
2. `config::get("BD")` entries from `config.yml`
3. Returns `NULL` if unresolved

`set_path()` stores paths in `BD_env` for the current R session only.

## Key convention: `.which` / `.path` / `.con` pattern
Every DB function accepts three optional connection parameters:
- `.which` — alias name looked up via `path()`
- `.path` — direct file path (must have `.sqlite`/`.sqlite3`/`.db` extension)
- `.con` — existing DBI connection (bypasses `connection()` entirely)

When `.con` is provided, the caller is responsible for disconnecting.

## Test infrastructure
- Test helper `tests/testthat/helper.R` provides `setup_config_env()` / `teardown_config_env()` to create a temporary `config.yml` fixture during tests.
- All tests use in-memory/tempfile SQLite databases with `on.exit()` cleanup.
- Use `devtools::test()` to run tests; `config.yml` at the package root is used for config-dependent tests.
- `covr::package_coverage()` also works because tests provision their own `config.yml`.

## Gotchas
- `BD_type()` (internal, in `R/path.R`) only recognizes `sqlite`, `sqlite3`, and `db` extensions. ACCDB paths will fail validation.
- `config.yml` is excluded from the built package via `.Rbuildignore`; deployed packages use their own `config::get("BD")`.
- `path()` calls `config::get("BD")` without `tryCatch` — if `config.yml` is absent, `path()` will error rather than return `NULL`. This is intentional: in production, a `config.yml` is always present. `list_paths()` handles the missing-config case gracefully via `tryCatch` for diagnostic use.
- `BD_type()` returns `NULL` (no `default` case) for unrecognized extensions — used as a validation gate in `path()` and `set_path()`. Extensions like `.accdb` or extensionless paths are intentionally rejected.
- `man/*.Rd` is in `.gitignore` — all man/ files are roxygen2-generated and should not be committed (except the `man/` directory structure itself).

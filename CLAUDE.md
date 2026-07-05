## What this package is

An **offline-first, secure data repository** for Dart/Flutter. The user supplies a `NetworkAdapter` (HTTP client) and a `StorageAdapter` (durable cache + sync queue); `ReliableRepository` wires them into an optimistic-write, cache-first read flow with retry + AES-at-rest.

Public surface (from `lib/reliable.dart`):

- `ReliableRepository` — the orchestrator
- `ReliableCore` types: `StorageAdapter`, `NetworkAdapter`, `ReliableEncryption`, `OfflineAction`, `CacheStrategy`, `RequestMethod`, `ReliableNetworkException`, `QueueCorruptionException`, `ReliableSchema`
- `SembastAdapter`, `SharedPreferencesAdapter` — default storage backends
- `ReliableAesEncryption` — AES-256 with random IV per call, signature `aes_v2`

`SqliteAdapter` exists at `lib/src/adapters/sqlite_adapter.dart` but is **deliberately not exported** from `reliable.dart` — `sqflite` pulls in `dart:ffi`, which breaks the web build. Mobile-only consumers import it directly:
```dart
import 'package:reliable/src/adapters/sqlite_adapter.dart';
```

## Storage-paradigm portability

The `StorageAdapter` contract is purely document-oriented: read/write by `(collection, id)`, both as strings; queue actions as opaque maps. No relations, joins, or query-language semantics ever leak into `ReliableRepository`. This is load-bearing — `reliable` is meant to work behind both SQL backends (the bundled `SqliteAdapter` stores each doc as a JSON-encoded `TEXT` row keyed on `(collection, doc_id)`) and NoSQL backends (the bundled `SembastAdapter` stores each doc as a native Map; a Firestore adapter would do the same). Anything new added to the repository must keep that property:

- Document IDs are always strings (no auto-increment integers, no compound keys leaking through the API).
- Anything that needs to be persisted goes into the doc/map body, not into adapter-specific columns. `ReliableSchema.colSchemaVersion` (`_reliable_schema_v`) lives inside the doc payload precisely so SQL adapters store it inside their JSON `colData` blob with zero DDL changes, and NoSQL adapters just have another field on the map.
- Cache migrations operate on the doc body only — they don't trigger storage-backend schema migrations (those are the adapter's `init(version:, onUpgrade:)` callback, which is a separate concern).
- Backward compatibility for persisted enums (e.g. `RequestMethod.GET` is the new on-disk form; old `'get'` entries still parse via case-insensitive match in `OfflineAction.fromMap`).

## Architecture invariants to preserve

These are load-bearing behaviours encoded in the code with comments — don't "clean them up" without understanding why.

### `_updateStorage` empty-list guard (`reliable_repository.dart`)

If a bulk `GET` returns `[]` while the in-memory cache is non-empty, `_updateStorage` **keeps disk intact** and logs `REPO_UPDATE_STORAGE_GUARDED`. Rationale: an empty 200 over a non-empty cache is almost always a transient server-side filter glitch (auth/scope race, identity drift, listing-query regression). Legitimate single deletions arrive via `applyServerDelete` (the SSE/server-push channel), so the bulk endpoint is never the right place to wipe a populated cache. `fetch` itself mirrors this guard and falls back to the cached list on net=[]. Removing either branch will silently empty user timelines on transient backend hiccups.

### `idField` injection on every write path

Every doc that lands in the cache (`fetchOne`, `applyServerChange`, `_updateMemory`, `_mergeOfflineChanges`) is rewritten as `{...data, idField: id}`. Envelope-style responses (no natural id in the body, e.g. `/v1/routines`) would otherwise be rehydrated under key `''` on the next boot — see the `_ensureMemCache` line:
```dart
(e[idField] ?? e[ReliableSchema.colDocId] ?? '').toString()
```
The `idField` parameter (default `'id'`) is how consumers point at MongoDB-style `_id` etc.

Consequence: when the cached document **is** the payload (a flat translation map, a config blob), the synthetic `id` key leaks into the caller's lookup namespace. `fetchOneRaw` (sibling of `fetchOne`) strips the injected key on the return path only — the on-disk copy still carries it, so rehydration keeps working. Use `fetchOneRaw` for "doc-as-payload" shapes (df_localization, flat config), and `fetchOne` for entity-shaped responses where the body already has a natural id.

### Sembast internal queue store name: `__reliable_action_queue`

The previous name `offline_queue` shared a Sembast store with any app collection of the same name (store identity is the *name*, the K type parameter is just a Dart-side hint). Apps writing String-keyed records to that collection caused every `_queueStore.find()` to throw `String is not a subtype of int` from Sembast's snapshot deserialization, crashing every `fetch` under `networkOrElseCache`. The double-underscore prefix prevents collision with app namespaces. Don't rename without a migration.

### Heavy diagnostic logging is intentional

`Log.alert` / `Log.info` / `Log.err` calls and the optional `auditLog` hook fire from every disk-touching path (`REPO_FETCH_*`, `REPO_UPDATE_STORAGE_*`, `REPO_APPLY_SERVER_DELETE`, `REPO_PURGE_COLLECTIONS`). The hook is wrapped in try/catch so a broken host hook can't fail the call site. These events exist to diagnose cache-wipe incidents — preserve event names if you refactor, since downstream apps grep for them.

### Cache migrations are per-collection and forward-only

`DataMigration` (in `reliable_core.dart`) is the on-disk doc-shape migration mechanism — separate from the storage adapter's `version`/`onUpgrade` callback, which is for the *storage backend's* schema. Each cached doc carries `ReliableSchema.colSchemaVersion` (`_reliable_schema_v`); on first read of a collection, `_ensureMemCache` walks the registered migrations in ascending `toVersion` order and brings every stale doc forward, re-persisting it. Fresh network docs are tagged with the latest version on write (via `_persistDoc`). The schema-version field is stripped before docs enter `_memCache`, so callers never see it. When no migrations are registered for a collection, the system is a true no-op (no field injected on disk, no copies on read) — this is the hot path for compledo today and must stay that way.

### Server-push methods bypass the queue

`applyServerChange` and `applyServerDelete` write straight to mem + disk and **do not** enqueue an `OfflineAction`. They're for the inbound SSE/webhook channel where the change already exists upstream. Don't add queue calls to them.

### `OfflineAction.fromMap` throws `QueueCorruptionException`, not `FormatException`

The housekeeping loop in `_processQueue` catches it and drops the bad row instead of stalling forever. New required fields on `OfflineAction` should route validation through the existing `requireString` / `requireInt` helpers so corrupt rows are recoverable.

## Platform-conditional sembast wiring

`sembast_adapter.dart` does a conditional import:
```dart
import 'sembast_adapter_stub.dart'
    if (dart.library.io) 'sembast_adapter_native.dart'
    if (dart.library.js_interop) 'sembast_adapter_web.dart'
    if (dart.library.js) 'sembast_adapter_web.dart'
    if (dart.library.html) 'sembast_adapter_web.dart' as platform;
```
The native variant uses `path_provider` + `sembast_io`; the web variant uses `sembast_web`; the stub throws. When adding a new platform-conditional dependency, mirror this four-clause pattern (one entry per relevant `dart.library` flavour) — `js_interop` alone is not sufficient on older Flutter web builds.

## Commands

Standard Dart/Flutter loop — this is a Flutter package (depends on `flutter` SDK), so use the Flutter variants:

```sh
flutter pub get
dart analyze
dart format .
flutter test
flutter test test/reliable_queue_test.dart
flutter test --plain-name "networkOrElseCache returns cache when network fails"
dart pub publish --dry-run
```

Tests live at `test/` **flat** (no mirrored `test/unit/src/...` tree like `df_safer_dart` uses). New tests for a module go next to the existing ones.

The tests use in-memory `_FakeStorage` / `_FakeNetwork` doubles defined inline in each `*_test.dart` — they do **not** spin up real Sembast or sqlite. That's intentional: the repository contract is what's under test, not the adapters. Adapter-level behavior (e.g. `sembast_store_name_collision_test.dart`) is tested against the real Sembast in-memory factory.

## Linting

This package uses `package:flutter_lints/recommended.yaml` as its base — **not** the workspace-canonical `dart_package_template/flutter_analysis_options.yaml`. `@scripts/standardize_all_analysis_options.ps1` will overwrite this if run; only run it if you intend to adopt the workspace baseline (which would add `custom_lint` + `df_safer_dart_lints` and strict-cast/inference/raw-type flags).

The `analyzer.plugins: [custom_lint]` block is present but no `custom_lint` dependency is declared — running `dart run custom_lint` here is a no-op.

## Release flow (overrides the workspace default)

This package uses a **branch-based** release, not the standard `+message`/`++message` commit-prefix flow:

```sh
./deploy.sh    # merges main → prod, pushes prod, switches back to main
```

The pub.dev publish is triggered by `.github/workflows/prod.yml` reacting to pushes on the `prod` branch. `publish.yml` is the secondary/standard workflow. Bump `version:` in `pubspec.yaml` and update `CHANGELOG.md` on `main` before running `deploy.sh`.

## Local dependency override

`pubspec_overrides.yaml` is checked in (the workspace `.gitignore` exempts this file isn't true here — it's tracked) and pins `df_log` to `../df_log`. If you run `@scripts/create_all_pubspec_overrides.ps1` from the workspace, expect this file to be regenerated; if you run `delete_all_pubspec_overrides.ps1` before publishing, it will be wiped and `df_log` will resolve to its pub.dev version.

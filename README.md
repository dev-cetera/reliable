[![pub](https://img.shields.io/pub/v/reliable.svg)](https://pub.dev/packages/reliable)
[![tag](https://img.shields.io/badge/Tag-v0.1.0-purple?logo=github)](https://github.com/dev-cetera/reliable/tree/v0.1.0)
[![buymeacoffee](https://img.shields.io/badge/Buy%20Me%20A%20Coffee-FFDD00?logo=buy-me-a-coffee&logoColor=black)](https://www.buymeacoffee.com/dev_cetera)
[![sponsor](https://img.shields.io/badge/Sponsor-grey?logo=github-sponsors&logoColor=pink)](https://github.com/sponsors/dev-cetera)
[![patreon](https://img.shields.io/badge/Patreon-grey?logo=patreon)](https://www.patreon.com/robelator)
[![discord](https://img.shields.io/badge/Discord-5865F2?logo=discord&logoColor=white)](https://discord.gg/gEQ8y2nfyX)
[![instagram](https://img.shields.io/badge/Instagram-E4405F?logo=instagram&logoColor=white)](https://www.instagram.com/dev_cetera/)
[![license](https://img.shields.io/badge/License-MIT-blue.svg)](https://raw.githubusercontent.com/dev-cetera/reliable/main/LICENSE)

---

<!-- BEGIN _README_CONTENT -->

## Summary

An offline-first, secure data repository for Flutter. You bring a `NetworkAdapter` (any REST-ish HTTP client) and pick a `StorageAdapter` (Sembast, sqflite, or SharedPreferences); `ReliableRepository` glues them together into an optimistic-write, cache-first read pipeline with retry, exponential backoff, AES-256 at rest, and a durable sync queue that survives app restarts.

- **Optimistic writes.** Mutations land in RAM + disk immediately and are queued for sync; the UI never blocks on the network.
- **Cache-first reads.** Four `CacheStrategy` modes (`cacheOrElseNetwork`, `networkOrElseCache`, `cacheOnly`, `networkOnly`) give you the right freshness vs. responsiveness trade-off per call.
- **Server-push integration.** `applyServerChange` / `applyServerDelete` let SSE/WebSocket handlers feed into the same cache without enqueueing fake actions.
- **AES-256 at rest.** Pluggable `ReliableEncryption` interface; the bundled `ReliableAesEncryption` uses a random IV per write.
- **User-scoped queueing.** Set the active user and pending actions for other users are skipped until that user is active again.

## Installation

```sh
flutter pub add reliable
```

> `reliable` is a Flutter package (it ships Flutter-only adapters for `path_provider` / `shared_preferences` / `sqflite`). Pure-Dart server code can't import it today — file an issue if you need a pure-Dart core split out.

## Quick start

```dart
import 'package:reliable/reliable.dart';

// 1. Implement NetworkAdapter for your HTTP client (Dio, http, custom, ...).
class MyApi implements NetworkAdapter {
  @override
  Future<dynamic> request(
    String endpoint,
    RequestMethod method, {
    dynamic data,
    Map<String, dynamic>? queryParams,
    Map<String, String>? headers,
  }) async {
    // ...call your HTTP client and return the decoded body
  }
}

// 2. Pick a storage backend, initialize, and construct the repository.
final storage = SembastAdapter(dbName: 'app.db');
await storage.init(version: 1, onUpgrade: (db, oldV, newV) async {});

final repo = ReliableRepository(api: MyApi(), storage: storage);
await repo.initialize(); // starts the 5s background sync heartbeat

// 3. Read with a cache strategy.
final todos = await repo.fetch(
  'todos',
  '/v1/todos',
  strategy: CacheStrategy.NETWORK_OR_ELSE_CACHE,
);

// 4. Write optimistically — queues for sync, returns immediately.
await repo.write(
  collection: 'todos',
  endpoint: '/v1/todos',
  method: RequestMethod.POST,
  data: {'title': 'Buy milk'},
);

// 5. Inspect / drain the queue.
final pending = await repo.getPendingCount();
await repo.syncNow(); // force a sync attempt instead of waiting for the heartbeat
```

A runnable Flutter demo with an "Online/Offline" toggle lives in [`example/`](example/lib/main.dart).

## Storage backends

| Adapter | Best for | Notes |
| --- | --- | --- |
| `SembastAdapter` | General-purpose, all platforms (mobile, desktop, web) | Default choice. Cross-platform via conditional imports. |
| `SharedPreferencesAdapter` | Tiny caches, key-value-shaped data | Lightweight; not suitable for large collections. |
| `SqliteAdapter` | Mobile-only, large datasets needing SQL | **Not exported from `reliable.dart`** — `sqflite` pulls in `dart:ffi`, which breaks web builds. Import directly: `import 'package:reliable/src/adapters/sqlite_adapter.dart';` |

## Encryption

Wrap any adapter with `ReliableAesEncryption` for transparent at-rest encryption:

```dart
final storage = SembastAdapter(dbName: 'app.db');
await storage.init(
  version: 1,
  onUpgrade: (db, oldV, newV) async {},
  encryption: ReliableAesEncryption('your-32-character-secret-key!!!'),
);
```

The key must be exactly 32 characters (AES-256). Each write uses a fresh random IV.

## Local cache migrations

When your backend's response shape changes between app releases, locally-cached docs from older app versions still live on the user's device. Without migration, the new app code crashes parsing them. Register a `DataMigration` per collection and `reliable` brings stale docs forward on the next read:

```dart
final repo = ReliableRepository(
  api: MyApi(),
  storage: storage,
  dataMigrations: {
    'users': [
      // v0 → v1: rename `firstName` → `name`.
      DataMigration(
        toVersion: 1,
        migrate: (doc) => {
          ...doc,
          'name': doc['firstName'],
        }..remove('firstName'),
      ),
      // v1 → v2: ensure `verified` is always present.
      DataMigration(
        toVersion: 2,
        migrate: (doc) => {'verified': false, ...doc},
      ),
    ],
  },
);
```

- Migrations are **forward-only** and applied in ascending `toVersion` order. Every cached doc's version is tracked under `ReliableSchema.colSchemaVersion` (a reserved internal field stripped from returned maps).
- Freshly-fetched network docs are tagged with the latest version on write — they're assumed to already match the current schema.
- A migration that throws drops just that doc and emits a `REPO_MIGRATION_FAILED` audit event; the rest of the collection still loads. The host can refetch from the network to recover.
- Collections with no migrations registered cost nothing: the schema-version field is never written and reads never copy.

## Using Dio (or any HTTP client) without a hard dependency

`reliable` does not depend on Dio, `http`, or any specific HTTP client. You provide a `NetworkAdapter` that wraps whatever your app already uses. Canonical Dio wrapper:

```dart
import 'package:dio/dio.dart';
import 'package:reliable/reliable.dart';

class DioNetworkAdapter implements NetworkAdapter {
  DioNetworkAdapter(this.dio);
  final Dio dio;

  @override
  Future<dynamic> request(
    String endpoint,
    RequestMethod method, {
    dynamic data,
    Map<String, dynamic>? queryParams,
    Map<String, String>? headers,
  }) async {
    try {
      final response = await dio.request<dynamic>(
        endpoint,
        data: data,
        queryParameters: queryParams,
        options: Options(method: method.name.toUpperCase(), headers: headers),
      );
      return response.data;
    } on DioException catch (e) {
      // 4xx (except 401/408/429) is permanent — don't waste queue retries on it.
      final code = e.response?.statusCode;
      final fatal = code != null &&
          code >= 400 &&
          code < 500 &&
          code != 401 &&
          code != 408 &&
          code != 429;
      throw ReliableNetworkException(
        message: e.message ?? 'request failed',
        statusCode: code,
        isFatal: fatal,
      );
    }
  }
}
```

The translation rule that matters: convert client/protocol errors into `ReliableNetworkException` with `isFatal` set correctly. Fatal errors drop the action from the queue immediately; non-fatal errors get exponential backoff. Returning the decoded body (Map, List, or null) is the only other contract — `reliable` handles both raw lists and `{data: [...]}` envelopes.

## Working with envelope-style responses

If your endpoint returns the document **as** the payload (a translation map keyed by translation key, a flat config blob, etc.), use `fetchOneRaw` so the internally-injected `id` field doesn't pollute your lookup namespace:

```dart
final translations = await repo.fetchOneRaw(
  'translations',
  '/v1/translations',
  id: 'en_US',
);
// translations is just {'hello': 'Hello', 'world': 'World', ...} — no synthetic 'id' key.
```

For entity-shaped responses (the body has its own `id`), use `fetchOne` as normal.

<!-- END _README_CONTENT -->

---

🔍 For more information, refer to the [API reference](https://pub.dev/documentation/reliable/).

---

## 💬 Contributing and Discussions

This is an open-source project, and we warmly welcome contributions from everyone, regardless of experience level. Whether you're a seasoned developer or just starting out, contributing to this project is a fantastic way to learn, share your knowledge, and make a meaningful impact on the community.

### ☝️ Ways you can contribute

- **Find us on Discord:** Feel free to ask questions and engage with the community here: https://discord.gg/gEQ8y2nfyX.
- **Share your ideas:** Every perspective matters, and your ideas can spark innovation.
- **Help others:** Engage with other users by offering advice, solutions, or troubleshooting assistance.
- **Report bugs:** Help us identify and fix issues to make the project more robust.
- **Suggest improvements or new features:** Your ideas can help shape the future of the project.
- **Help clarify documentation:** Good documentation is key to accessibility. You can make it easier for others to get started by improving or expanding our documentation.
- **Write articles:** Share your knowledge by writing tutorials, guides, or blog posts about your experiences with the project. It's a great way to contribute and help others learn.

No matter how you choose to contribute, your involvement is greatly appreciated and valued!

### ☕ We drink a lot of coffee...

If you're enjoying this package and find it valuable, consider showing your appreciation with a small donation. Every bit helps in supporting future development. You can donate here: https://www.buymeacoffee.com/dev_cetera

<a href="https://www.buymeacoffee.com/dev_cetera" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/default-orange.png" height="40"></a>

## LICENSE

This project is released under the [MIT License](https://raw.githubusercontent.com/dev-cetera/reliable/main/LICENSE). See [LICENSE](https://raw.githubusercontent.com/dev-cetera/reliable/main/LICENSE) for more information.

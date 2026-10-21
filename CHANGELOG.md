# CHANGELOG

## [0.2.0]

- Released @ 10/2026 (UTC)
- BREAKING: `fetch` now throws a `FormatException` (naming the keys it saw) for a list answer of unknown shape, instead of silently reading it as `[]` and serving stale cache forever; a list entry that is not an object also throws a `FormatException`. `{items: [...]}` is now unwrapped alongside a bare array and `{data: [...]}`; a `null` body still reads as `[]`
- A read whose answer lands after `purgeCollections`, `clearUserData` or a `setActiveUser` switch to another user no longer writes it into the cache (the caller still gets the answer); a purge arriving part-way through a multi-step cache write stops it, and the purge clears what landed
- `SembastAdapter` reopens a dead database connection (iOS Safari killing IndexedDB under a backgrounded tab) and retries the operation once, giving up for the session if the reopen fails, hangs past `reopenTimeout`, or is followed straight away by another dead-connection failure; new optional constructor parameters `reopenTimeout` and `openDatabase`, plus `recoveryGaveUp` and `SembastAdapter.isDeadConnectionError`
- A failed read served from cache (`NETWORK_OR_ELSE_CACHE`) now logs at `Log.alert` instead of `Log.err` when the request got no answer at all (offline, dropped connection, timeout); `Log.err` remains when nothing answered from cache, the error propagates, the server answered with an error status (`ReliableNetworkException.statusCode` set, e.g. 401 or 5xx), or the answer was malformed
- A good read whose cache copy cannot be saved is returned as a success, with the failed save logged as a `Log.alert` warning (`REPO_CACHE_WRITE_FAILED`) instead of failing the read

## [0.1.1]

- Update dependencies

## [0.1.0]

- Initial release
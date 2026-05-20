import 'dart:async';
import 'dart:math';
import 'package:df_log/df_log.dart';
import 'package:uuid/uuid.dart';
import 'package:synchronized/synchronized.dart';
import 'reliable_core.dart';

class ReliableRepository {
  final NetworkAdapter _api;
  final StorageAdapter<dynamic> _storage;

  /// The key used to extract document IDs from API response maps.
  /// Defaults to `'id'`. Change to `'_id'` for MongoDB-style APIs, etc.
  final String idField;

  /// Maximum number of retry attempts before an action is dropped from the queue.
  final int maxRetries;

  /// Called when a queued action is successfully synced to the server.
  final void Function(OfflineAction action)? onSyncSuccess;

  /// Called when a queued action permanently fails (fatal error or max retries).
  final void Function(OfflineAction action, Object error)? onSyncError;

  /// Optional persistent audit hook. Called from every disk-touching path
  /// (fetch outcomes, _updateStorage GUARD/SHRINK/EMPTY-WRITE, fetchOne
  /// OVERWRITE, purgeCollections, applyServerDelete). Lets the host app
  /// route these into a localStorage-backed trail so the events survive
  /// across app restarts and are visible in devtools on the wipe boot.
  final void Function(String event, Map<String, dynamic>? details)? auditLog;

  final _lock = Lock();
  Timer? _heartbeat;

  final Map<String, Map<String, Map<String, dynamic>>> _memCache = {};
  final Set<String> _loadedCollections = {};
  String? _currentUserId;

  ReliableRepository({
    required NetworkAdapter api,
    required StorageAdapter<dynamic> storage,
    this.idField = 'id',
    this.maxRetries = 10,
    this.onSyncSuccess,
    this.onSyncError,
    this.auditLog,
  }) : _api = api,
       _storage = storage;

  void _audit(String event, [Map<String, dynamic>? details]) {
    final hook = auditLog;
    if (hook != null) {
      try {
        hook(event, details);
      } catch (error) {
        // Audit must never fail the call site, but a silently-broken audit
        // hook will hide other bugs. Log the throw so it's at least visible.
        Log.err('reliable audit hook threw on "$event": $error');
      }
    }
  }

  /// Starts the background sync heartbeat. Call once after construction.
  Future<void> initialize({
    Duration heartbeatDuration = const Duration(seconds: 5),
  }) async {
    await _performHousekeeping();
    _heartbeat = Timer.periodic(heartbeatDuration, (_) => _processQueue());
  }

  /// Cancels the background sync heartbeat.
  void dispose() {
    _heartbeat?.cancel();
  }

  // --- AUTH ---

  /// Sets the active user context. Only actions for this user will be synced.
  void setActiveUser(String? userId) {
    _currentUserId = userId;
    if (userId != null) _processQueue();
  }

  /// Returns the currently active user ID.
  String? get currentUserId => _currentUserId;

  /// Clears the in-memory cache and optionally removes pending actions for the
  /// given user. Typically called on logout.
  Future<void> clearUserData(
    String userId, {
    bool clearPendingActions = true,
  }) async {
    _memCache.clear();
    _loadedCollections.clear();
    if (_currentUserId == userId) _currentUserId = null;
    if (clearPendingActions) await _storage.removeActionsForUser(userId);
  }

  /// Snapshot the current disk + memCache size for each [collections]. Used
  /// by the diagnostic boot audit so we can see exactly how many rows live
  /// in each collection before/after key transitions (boot, login, logout,
  /// network failure). Reads disk through [_storage.readCollection] so the
  /// snapshot is correct even before the in-memory cache has loaded.
  Future<Map<String, ({int disk, int mem})>> snapshotCollectionSizes(
    List<String> collections,
  ) async {
    final out = <String, ({int disk, int mem})>{};
    for (final collection in collections) {
      var disk = -1;
      try {
        final list = await _storage.readCollection(collection: collection);
        disk = list.length;
      } catch (error) {
        // -1 signals "read failed" (storage error) so callers can tell it
        // apart from a legitimately empty collection. Log so we don't lose
        // the underlying cause when investigating a wipe report.
        Log.err('reliable snapshotCollectionSizes read failed for $collection: $error');
      }
      final mem = _memCache[collection]?.length ?? -1;
      out[collection] = (disk: disk, mem: mem);
    }
    return out;
  }

  /// Permanently delete every document in [collections] from disk and memory.
  /// Use this to wipe the durable Sembast records that survive
  /// [clearUserData] (which only touches the in-memory cache and the action
  /// queue). Caller is responsible for naming only the collections that
  /// belong to the namespace being purged — collections aren't user-scoped
  /// at the storage layer.
  Future<void> purgeCollections(List<String> collections) async {
    final stack = StackTrace.current.toString().split('\n').take(6).join(' | ');
    Log.alert(
      'purgeCollections wiping disk for $collections — stack=$stack',
    );
    _audit('REPO_PURGE_COLLECTIONS', {
      'collections': collections,
      'stack': stack,
    });
    for (final collection in collections) {
      _memCache.remove(collection);
      _loadedCollections.remove(collection);
      await _storage.clearCollection(collection: collection);
    }
  }

  // --- READ (Collection) ---

  /// Fetches a list of documents from [collection] using the given [endpoint].
  ///
  /// The [strategy] controls the read path (cache vs. network).
  /// Pass [queryParams] to forward query parameters to the network request.
  Future<List<Map<String, dynamic>>> fetch(
    String collection,
    String endpoint, {
    CacheStrategy strategy = CacheStrategy.cacheOrElseNetwork,
    Map<String, dynamic>? queryParams,
  }) async {
    await _ensureMemCache(collection);

    List<Map<String, dynamic>> getLocal() {
      return _memCache[collection]?.values.toList() ?? [];
    }

    if (strategy == CacheStrategy.cacheOrElseNetwork ||
        strategy == CacheStrategy.cacheOnly) {
      final local = getLocal();
      if (local.isNotEmpty || strategy == CacheStrategy.cacheOnly) {
        return await _mergeOfflineChanges(collection, local);
      }
    }

    try {
      final response = await _api.request(
        endpoint,
        RequestMethod.get,
        queryParams: queryParams,
      );
      final netList = _parseResponse(response);
      Log.info(
        'fetch $collection $endpoint → net=${netList.length} '
        'memCache=${_memCache[collection]?.length ?? 0}',
      );
      _audit('REPO_FETCH_NET', {
        'collection': collection,
        'endpoint': endpoint,
        'net': netList.length,
        'memCache': _memCache[collection]?.length ?? 0,
      });
      await _updateStorage(collection, netList);
      // Mirror the disk guard in _updateStorage: a 200 with an empty body
      // over a non-empty cache is almost always a transient server-side
      // filter glitch (auth/scope race, identity drift, listing-query
      // regression). Returning [] here would render an empty timeline even
      // though the cache (and the database) still hold the data. Single
      // deletions arrive via applyServerDelete, so the bulk-list endpoint
      // is not the legitimate channel for a real "everything is gone" event.
      if (netList.isEmpty) {
        final local = getLocal();
        if (local.isNotEmpty) {
          Log.alert(
            'fetch $collection: net=[] '
            'falling back to ${local.length} cached',
          );
          _audit('REPO_FETCH_NET_EMPTY_FALLBACK_CACHE', {
            'collection': collection,
            'cacheSize': local.length,
          });
          return await _mergeOfflineChanges(collection, local);
        }
      }
      return await _mergeOfflineChanges(collection, netList);
    } catch (error) {
      Log.err(
        'fetch $collection $endpoint threw (${error.runtimeType}): '
        '$error — strategy=$strategy memCache=${_memCache[collection]?.length ?? 0}',
      );
      _audit('REPO_FETCH_THREW', {
        'collection': collection,
        'endpoint': endpoint,
        'errorType': '${error.runtimeType}',
        'error': '$error',
        'strategy': '$strategy',
        'memCache': _memCache[collection]?.length ?? 0,
      });
      if (strategy == CacheStrategy.networkOrElseCache) {
        return await _mergeOfflineChanges(collection, getLocal());
      }
      rethrow;
    }
  }

  // --- READ (Single Document) ---

  /// Fetches a single document by [id] from [collection].
  ///
  /// Returns `null` if the document is not found in cache (for `cacheOnly`)
  /// or if the network returns a non-Map response.
  Future<Map<String, dynamic>?> fetchOne(
    String collection,
    String endpoint, {
    required String id,
    CacheStrategy strategy = CacheStrategy.cacheOrElseNetwork,
    Map<String, dynamic>? queryParams,
  }) async {
    await _ensureMemCache(collection);

    if (strategy == CacheStrategy.cacheOrElseNetwork ||
        strategy == CacheStrategy.cacheOnly) {
      final cached = _memCache[collection]?[id];
      if (cached != null || strategy == CacheStrategy.cacheOnly) return cached;
    }

    try {
      final response = await _api.request(
        endpoint,
        RequestMethod.get,
        queryParams: queryParams,
      );
      if (response is Map) {
        Map<String, dynamic> doc;
        if (response['data'] is Map) {
          doc = Map<String, dynamic>.from(response['data'] as Map);
        } else {
          doc = Map<String, dynamic>.from(response);
        }
        // Inject the lookup id so envelope-style responses (no natural id
        // field, e.g. /v1/routines) survive disk rehydration. Without this,
        // _ensureMemCache re-keys the doc under '' on the next boot and
        // fetchOne(id: ...) returns null even though the data is on disk.
        final docWithId = {...doc, idField: id};
        final cached = _memCache[collection]?[id];
        if (cached != null) {
          Log.info(
            'fetchOne $collection/$id $endpoint overwrite: '
            'cached keys=${cached.keys.toList()} → '
            'net keys=${docWithId.keys.toList()} '
            '(cached size=${cached.length} net size=${docWithId.length})',
          );
          _audit('REPO_FETCH_ONE_OVERWRITE', {
            'collection': collection,
            'id': id,
            'cachedKeys': cached.keys.toList(),
            'netKeys': docWithId.keys.toList(),
          });
        } else {
          Log.info(
            'fetchOne $collection/$id $endpoint initial: '
            'net keys=${docWithId.keys.toList()}',
          );
          _audit('REPO_FETCH_ONE_INITIAL', {
            'collection': collection,
            'id': id,
            'netKeys': docWithId.keys.toList(),
          });
        }
        _memCache[collection] ??= {};
        _memCache[collection]![id] = docWithId;
        await _storage.writeDocument(
          collection: collection,
          id: id,
          data: docWithId,
        );
        return docWithId;
      }
      return null;
    } catch (error) {
      Log.err(
        'fetchOne $collection/$id $endpoint threw '
        '(${error.runtimeType}): $error — '
        'cached=${_memCache[collection]?[id] != null}',
      );
      _audit('REPO_FETCH_ONE_THREW', {
        'collection': collection,
        'id': id,
        'endpoint': endpoint,
        'errorType': '${error.runtimeType}',
        'error': '$error',
        'cached': _memCache[collection]?[id] != null,
      });
      if (strategy == CacheStrategy.networkOrElseCache) {
        return _memCache[collection]?[id];
      }
      rethrow;
    }
  }

  // --- WRITE ---

  /// Writes data to the local cache and queues the action for background sync.
  ///
  /// The [id] is auto-generated if not provided (for POST/create operations).
  /// Set [timeToLive] to automatically expire the action if it hasn't synced.
  Future<void> write({
    required String collection,
    required String endpoint,
    required RequestMethod method,
    required Map<String, dynamic> data,
    String? id,
    Duration? timeToLive,
  }) async {
    final docId = id ?? const Uuid().v4();
    final uuid = const Uuid().v4();
    final now = DateTime.now().millisecondsSinceEpoch;

    // Optimistic update (RAM)
    _updateMemory(collection, docId, method, data);

    // Persist to queue (Disk)
    final action = OfflineAction(
      uuid: uuid,
      collection: collection,
      endpoint: endpoint,
      method: method,
      docId: docId,
      payload: data,
      timestamp: now,
      userId: _currentUserId,
      expiresAt: timeToLive != null ? now + timeToLive.inMilliseconds : null,
      nextTryAt: 0,
    );

    await _storage.queueAction(action);
    unawaited(_processQueue());
  }

  // --- DELETE (Convenience) ---

  /// Convenience method to delete a document. Equivalent to calling [write]
  /// with [RequestMethod.delete].
  Future<void> delete({
    required String collection,
    required String endpoint,
    required String id,
    Duration? timeToLive,
  }) {
    return write(
      collection: collection,
      endpoint: endpoint,
      method: RequestMethod.delete,
      data: const {},
      id: id,
      timeToLive: timeToLive,
    );
  }

  // --- QUEUE INSPECTION ---

  /// Returns all pending actions in the sync queue.
  Future<List<OfflineAction>> getPendingActions() => _storage.getQueue();

  /// Returns the number of pending actions in the sync queue.
  Future<int> getPendingCount() async => (await _storage.getQueue()).length;

  /// Force an immediate sync attempt, bypassing the heartbeat timer.
  Future<void> syncNow() => _processQueue();

  // --- SERVER-PUSH CACHE INTEGRATION ---

  /// Upsert a document into the local cache from an external source (e.g. SSE).
  /// This does NOT queue a sync action — the change already exists on the server.
  Future<void> applyServerChange({
    required String collection,
    required String id,
    required Map<String, dynamic> data,
  }) async {
    await _ensureMemCache(collection);
    // Inject the lookup id so envelope-style writes (no natural id field)
    // survive disk rehydration — _ensureMemCache keys by data[idField].
    final docWithId = {...data, idField: id};
    _memCache[collection] ??= {};
    _memCache[collection]![id] = docWithId;
    await _storage.writeDocument(
      collection: collection,
      id: id,
      data: docWithId,
    );
  }

  /// Remove a document from the local cache when the server reports deletion.
  /// This does NOT queue a sync action.
  Future<void> applyServerDelete({
    required String collection,
    required String id,
  }) async {
    // Caller-attributable: every items/routines deletion goes through here
    // (SSE handlers, items.start ghost-cleanup, deleteItem confirm) — a
    // stack lets us name the path that fired on the wipe boot.
    final stack = StackTrace.current.toString().split('\n').take(6).join(' | ');
    _audit('REPO_APPLY_SERVER_DELETE', {
      'collection': collection,
      'id': id,
      'memSizeBefore': _memCache[collection]?.length ?? -1,
      'stack': stack,
    });
    await _ensureMemCache(collection);
    _memCache[collection]?.remove(id);
    await _storage.deleteDocument(collection: collection, id: id);
  }

  // --- SYNC ENGINE ---

  Future<void> _processQueue() async {
    if (_lock.inLock) return;

    try {
      await _lock.synchronized(() async {
        await _performHousekeeping();
        final queue = await _storage.getQueue();
        if (queue.isEmpty) return;

        final now = DateTime.now().millisecondsSinceEpoch;

        for (final action in queue) {
          if (action.userId != null && action.userId != _currentUserId) {
            continue;
          }
          if (action.nextTryAt > now) continue;

          try {
            var url = action.endpoint;
            if (action.method != RequestMethod.post) {
              url = '${action.endpoint}/${action.docId}';
            }

            await _api.request(url, action.method, data: action.payload);
            await _storage.removeFromQueue(action.uuid);
            onSyncSuccess?.call(action);
          } catch (error) {
            await _handleSyncError(action, error);
          }
        }
      });
    } catch (error) {
      // Swallow errors to prevent unhandled exceptions from the heartbeat.
      // The next heartbeat tick will retry — but log so a persistently
      // broken queue surface is visible rather than silently retried forever.
      Log.err('reliable processQueue heartbeat threw: $error');
    }
  }

  Future<void> _handleSyncError(OfflineAction action, Object error) async {
    // Fatal errors (e.g. 400 Bad Request) - remove immediately, no point retrying.
    if (error is ReliableNetworkException && error.isFatal) {
      await _storage.removeFromQueue(action.uuid);
      onSyncError?.call(action, error);
      return;
    }

    final attempts = action.retryCount + 1;

    // Max retries exceeded - give up.
    if (attempts >= maxRetries) {
      await _storage.removeFromQueue(action.uuid);
      onSyncError?.call(action, error);
      return;
    }

    // Schedule retry with exponential backoff.
    final backoffMs = _calculateBackoffMs(attempts);
    final updated = action.copyWith(
      retryCount: attempts,
      nextTryAt: DateTime.now().millisecondsSinceEpoch + backoffMs,
    );
    await _storage.updateQueueItem(updated);
  }

  Future<void> _performHousekeeping() => _storage.removeExpiredActions();

  /// Upper bound on the exponential backoff (1 hour); past this, the delay
  /// is capped so failing actions don't drift weeks into the future.
  static const int _kMaxBackoffSeconds = 3600;

  /// Width of the random jitter applied on top of the exponential delay,
  /// to spread retry pile-ups across the fleet.
  static const int _kJitterWindowMs = 1000;

  /// Returns backoff delay in milliseconds: 2^attempts seconds + random jitter.
  int _calculateBackoffMs(int attempts) {
    final delaySeconds = min(pow(2, attempts).toInt(), _kMaxBackoffSeconds);
    final jitterMs = Random().nextInt(_kJitterWindowMs);
    return (delaySeconds * 1000) + jitterMs;
  }

  // --- HELPERS ---

  Future<void> _ensureMemCache(String collection) async {
    if (!_loadedCollections.contains(collection)) {
      final diskData = await _storage.readCollection(collection: collection);
      _memCache[collection] = {
        for (var e in diskData)
          (e[idField] ?? e[ReliableSchema.colDocId] ?? '').toString(): e,
      };
      _loadedCollections.add(collection);
    }
  }

  void _updateMemory(
    String collection,
    String docId,
    RequestMethod method,
    Map<String, dynamic> data,
  ) {
    _memCache[collection] ??= {};
    if (method == RequestMethod.delete) {
      _memCache[collection]!.remove(docId);
    } else if (method == RequestMethod.patch) {
      final existing = _memCache[collection]![docId] ?? {};
      _memCache[collection]![docId] = {...existing, ...data, idField: docId};
    } else {
      _memCache[collection]![docId] = {...data, idField: docId};
    }
  }

  Future<void> _updateStorage(
    String collection,
    List<Map<String, dynamic>> list,
  ) async {
    // An empty bulk response over a non-empty cache is almost always a
    // transient server-side filter glitch (auth/scope race, identity drift,
    // a regression in the listing query). Wiping the cache here turns a
    // transient empty into a permanent one because `networkOrElseCache` then
    // has nothing left to fall back to. Single-item deletions arrive via
    // `applyServerDelete` (SSE), so a bulk-empty response shouldn't be the
    // channel for legitimate deletions.
    final existing = _memCache[collection];
    if (list.isEmpty && existing != null && existing.isNotEmpty) {
      Log.alert(
        '_updateStorage guarded $collection: '
        'net=[] memCache=${existing.length} (kept disk)',
      );
      _audit('REPO_UPDATE_STORAGE_GUARDED', {
        'collection': collection,
        'memCache': existing.length,
      });
      _loadedCollections.add(collection);
      return;
    }

    final replacingCount = existing?.length ?? 0;
    if (replacingCount > 0 && list.length < replacingCount) {
      Log.alert(
        '_updateStorage shrink $collection: '
        'memCache=$replacingCount → net=${list.length} (disk wiped + replaced)',
      );
      _audit('REPO_UPDATE_STORAGE_SHRINK', {
        'collection': collection,
        'before': replacingCount,
        'after': list.length,
      });
    } else if (list.isEmpty) {
      Log.info(
        '_updateStorage empty-write $collection: '
        'memCache=$replacingCount net=0 (no-op clear)',
      );
      _audit('REPO_UPDATE_STORAGE_EMPTY_WRITE', {
        'collection': collection,
        'memCache': replacingCount,
      });
    }

    _memCache[collection] = {
      for (var e in list) (e[idField] ?? '').toString(): e,
    };
    _loadedCollections.add(collection);
    await _storage.clearCollection(collection: collection);
    for (var item in list) {
      final docId = (item[idField] ?? '').toString();
      await _storage.writeDocument(
        collection: collection,
        id: docId,
        data: item,
      );
    }
  }

  Future<List<Map<String, dynamic>>> _mergeOfflineChanges(
    String collection,
    List<Map<String, dynamic>> source,
  ) async {
    final queue = await _storage.getQueue();
    final pending = queue
        .where(
          (action) =>
              action.collection == collection &&
              (action.userId == null || action.userId == _currentUserId),
        )
        .toList();

    if (pending.isEmpty) return source;

    final map = <String, Map<String, dynamic>>{
      for (var e in source) (e[idField] ?? '').toString(): e,
    };

    for (final action in pending) {
      if (action.method == RequestMethod.delete) {
        map.remove(action.docId);
      } else if (action.method == RequestMethod.post ||
          action.method == RequestMethod.put) {
        if (action.payload != null) {
          map[action.docId] = {...action.payload!, idField: action.docId};
        }
      } else if (action.method == RequestMethod.patch) {
        if (map.containsKey(action.docId) && action.payload != null) {
          map[action.docId] = {...map[action.docId]!, ...action.payload!};
        }
      }
    }
    return map.values.toList();
  }

  List<Map<String, dynamic>> _parseResponse(dynamic response) {
    if (response is List) {
      return List<Map<String, dynamic>>.from(
        response.map((e) => Map<String, dynamic>.from(e as Map)),
      );
    }
    if (response is Map) {
      final data = response['data'];
      if (data is List) {
        return List<Map<String, dynamic>>.from(
          data.map((e) => Map<String, dynamic>.from(e as Map)),
        );
      }
      if (data is Map) {
        return [Map<String, dynamic>.from(data)];
      }
    }
    return [];
  }
}

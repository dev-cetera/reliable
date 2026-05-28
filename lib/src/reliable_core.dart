/// Encryption interface for securing data at rest.
abstract interface class ReliableEncryption {
  String get signature;
  String encrypt(String input);
  String decrypt(String input);
}

/// Thrown by [OfflineAction.fromMap] when a queue row can't be parsed as a
/// well-formed action — typically the result of a Sembast schema migration
/// or a hand-edit. The repository's housekeeping loop catches this and drops
/// the bad row instead of stalling the queue forever.
class QueueCorruptionException implements Exception {
  QueueCorruptionException(this.message);
  final String message;
  @override
  String toString() => 'QueueCorruptionException: $message';
}

/// DB schema definitions.
final class ReliableSchema {
  const ReliableSchema._();

  static const String tableCache = 'reliable_cache';
  static const String tableQueue = 'reliable_queue';

  static const String colUuid = 'uuid';
  static const String colCollection = 'collection';
  static const String colDocId = 'doc_id';
  static const String colData = 'data';

  static const String colEndpoint = 'endpoint';
  static const String colMethod = 'method';
  static const String colPayload = 'payload';
  static const String colTimestamp = 'timestamp';
  static const String colRetryCount = 'retry_count';
  static const String colNextTryAt = 'next_try_at';
  static const String colUserId = 'user_id';
  static const String colExpiresAt = 'expires_at';

  /// Reserved field name written into every cached document to record its
  /// schema version. Used by [DataMigration] to detect docs that need to be
  /// brought forward. Stripped from the in-memory cache so callers never
  /// see it. Never store data under this key in your own documents — it
  /// will be overwritten.
  static const String colSchemaVersion = '_reliable_schema_v';
}

/// A single forward-only transformation that brings a cached document
/// from schema version `toVersion - 1` to [toVersion].
///
/// Cache migrations exist so locally-persisted documents survive backend
/// schema changes when an installed app is upgraded. They run on the next
/// read of each collection: every cached doc whose recorded version is
/// below the highest registered [toVersion] is passed through every
/// pending migration in ascending order, then re-persisted at the latest
/// version. Fresh responses from the network are assumed to already be
/// at the latest schema and are tagged on write.
///
/// Migrations must be:
/// - **Pure**: depend only on [doc] — no global state, no network.
/// - **Total**: handle missing or null fields rather than throwing. A
///   doc whose migration throws is dropped on the next read (and the
///   `REPO_MIGRATION_FAILED` audit event fires so the host can decide
///   whether to refetch from the network).
/// - **Forward-only**: there is no down-migration. Bump versions
///   monotonically across app releases.
final class DataMigration {
  /// The schema version this migration produces. Must be `>= 1` and
  /// unique within the per-collection migration list.
  final int toVersion;

  /// Transforms a doc from version `(toVersion - 1)` to [toVersion].
  final Map<String, dynamic> Function(Map<String, dynamic> doc) migrate;

  const DataMigration({required this.toVersion, required this.migrate});
}

/// Cache strategy for read operations.
enum CacheStrategy {
  /// (Default) Try RAM -> Disk -> Network. Fast, offline-friendly.
  CACHE_OR_ELSE_NETWORK,

  /// Try Network first, fall back to Disk on failure. Prioritizes freshness.
  NETWORK_OR_ELSE_CACHE,

  /// Disk/RAM only. Never hits the network.
  CACHE_ONLY,

  /// Network only. Never reads from cache (still writes to it).
  NETWORK_ONLY,
}

/// HTTP Request Methods.
enum RequestMethod { GET, POST, PUT, PATCH, DELETE }

/// Exception class for network-related errors specific to Reliable.
class ReliableNetworkException implements Exception {
  final String message;
  final int? statusCode;
  final bool isFatal;

  const ReliableNetworkException({
    required this.message,
    this.statusCode,
    this.isFatal = false,
  });

  @override
  String toString() =>
      'ReliableNetworkException($statusCode, fatal=$isFatal): $message';
}

/// Data class representing an offline action queued for synchronization.
final class OfflineAction {
  final String uuid;
  final String collection;
  final String endpoint;
  final RequestMethod method;
  final String docId;
  final Map<String, dynamic>? payload;
  final int timestamp;
  final int retryCount;
  final int nextTryAt;
  final String? userId;
  final int? expiresAt;

  const OfflineAction({
    required this.uuid,
    required this.collection,
    required this.endpoint,
    required this.method,
    required this.docId,
    this.payload,
    required this.timestamp,
    this.retryCount = 0,
    this.nextTryAt = 0,
    this.userId,
    this.expiresAt,
  });

  Map<String, dynamic> toMap() {
    return {
      ReliableSchema.colUuid: uuid,
      ReliableSchema.colCollection: collection,
      ReliableSchema.colEndpoint: endpoint,
      ReliableSchema.colMethod: method.name,
      ReliableSchema.colDocId: docId,
      ReliableSchema.colPayload: payload,
      ReliableSchema.colTimestamp: timestamp,
      ReliableSchema.colRetryCount: retryCount,
      ReliableSchema.colNextTryAt: nextTryAt,
      ReliableSchema.colUserId: userId,
      ReliableSchema.colExpiresAt: expiresAt,
    };
  }

  factory OfflineAction.fromMap(Map<String, dynamic> map) {
    String requireString(String key) {
      final value = map[key];
      if (value is! String) {
        throw QueueCorruptionException(
          'expected String for "$key", got ${value.runtimeType}',
        );
      }
      return value;
    }

    int requireInt(String key) {
      final value = map[key];
      if (value is! int) {
        throw QueueCorruptionException(
          'expected int for "$key", got ${value.runtimeType}',
        );
      }
      return value;
    }

    final rawPayload = map[ReliableSchema.colPayload];
    final Map<String, dynamic>? payload;
    if (rawPayload == null) {
      payload = null;
    } else if (rawPayload is Map) {
      payload = Map<String, dynamic>.from(rawPayload);
    } else {
      throw QueueCorruptionException(
        'expected Map for "${ReliableSchema.colPayload}", '
        'got ${rawPayload.runtimeType}',
      );
    }

    return OfflineAction(
      uuid: requireString(ReliableSchema.colUuid),
      collection: requireString(ReliableSchema.colCollection),
      endpoint: requireString(ReliableSchema.colEndpoint),
      method: () {
        final raw = map[ReliableSchema.colMethod]?.toString();
        if (raw == null) {
          throw QueueCorruptionException(
            'missing "${ReliableSchema.colMethod}"',
          );
        }
        // Case-insensitive match: persisted entries from versions prior to
        // the UPPER_SNAKE_CASE rename still carry lowercase names. New
        // writes use the uppercase form via [RequestMethod.name].
        final needle = raw.toUpperCase();
        final match = RequestMethod.values.where((e) => e.name == needle);
        if (match.isEmpty) {
          throw FormatException(
            'Unknown RequestMethod "$raw" in persisted OfflineAction',
          );
        }
        return match.first;
      }(),
      docId: requireString(ReliableSchema.colDocId),
      payload: payload,
      timestamp: requireInt(ReliableSchema.colTimestamp),
      retryCount: map[ReliableSchema.colRetryCount] as int? ?? 0,
      nextTryAt: map[ReliableSchema.colNextTryAt] as int? ?? 0,
      userId: map[ReliableSchema.colUserId] as String?,
      expiresAt: map[ReliableSchema.colExpiresAt] as int?,
    );
  }

  OfflineAction copyWith({int? retryCount, int? nextTryAt}) {
    return OfflineAction(
      uuid: uuid,
      collection: collection,
      endpoint: endpoint,
      method: method,
      docId: docId,
      payload: payload,
      timestamp: timestamp,
      userId: userId,
      expiresAt: expiresAt,
      retryCount: retryCount ?? this.retryCount,
      nextTryAt: nextTryAt ?? this.nextTryAt,
    );
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is OfflineAction &&
          uuid == other.uuid &&
          collection == other.collection &&
          endpoint == other.endpoint &&
          method == other.method &&
          docId == other.docId &&
          timestamp == other.timestamp &&
          retryCount == other.retryCount &&
          nextTryAt == other.nextTryAt &&
          userId == other.userId &&
          expiresAt == other.expiresAt;

  @override
  int get hashCode => Object.hash(
        uuid,
        collection,
        endpoint,
        method,
        docId,
        timestamp,
        retryCount,
        nextTryAt,
        userId,
        expiresAt,
      );

  @override
  String toString() => 'OfflineAction(uuid: $uuid, collection: $collection, '
      'method: ${method.name}, docId: $docId, retryCount: $retryCount)';
}

typedef MigrationCallback<T> = Future<void> Function(
    T db, int oldVersion, int newVersion);

/// An adapter interface for storage backends.
abstract interface class StorageAdapter<T> {
  Future<void> init({
    ReliableEncryption? encryption,
    required int version,
    required MigrationCallback<T> onUpgrade,
  });

  /// Reads all items from the specified collection.
  Future<List<Map<String, dynamic>>> readCollection({
    required String collection,
  });

  /// Writes or updates a specific item in the specified collection.
  Future<void> writeDocument({
    required String collection,
    required String id,
    required Map<String, dynamic> data,
  });

  /// Deletes a specific item from the specified collection.
  Future<void> deleteDocument({required String collection, required String id});

  /// Clears all data in the specified collection.
  Future<void> clearCollection({required String collection});

  Future<void> queueAction(OfflineAction action);
  Future<void> updateQueueItem(OfflineAction action);
  Future<List<OfflineAction>> getQueue();
  Future<void> removeFromQueue(String uuid);
  Future<void> removeExpiredActions();
  Future<void> removeActionsForUser(String userId);
}

/// An adapter interface for network backends.
abstract interface class NetworkAdapter {
  Future<dynamic> request(
    String endpoint,
    RequestMethod method, {
    dynamic data,
    Map<String, dynamic>? queryParams,
    Map<String, String>? headers,
  });
}

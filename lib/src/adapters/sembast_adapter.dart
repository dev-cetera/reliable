import 'dart:async';
import 'dart:convert';
import 'package:df_log/df_log.dart';
import 'package:sembast/sembast.dart';
import '../reliable_core.dart';

import 'sembast_adapter_stub.dart'
    if (dart.library.io) 'sembast_adapter_native.dart'
    if (dart.library.js_interop) 'sembast_adapter_web.dart'
    if (dart.library.js) 'sembast_adapter_web.dart'
    if (dart.library.html) 'sembast_adapter_web.dart' as platform;

/// Opens the Sembast database behind a [SembastAdapter]. The default opens
/// the platform's database (a file in the app documents directory on
/// native, IndexedDB on web). Pass your own to [SembastAdapter] to open a
/// different factory, e.g. `databaseFactoryMemory` in tests.
typedef SembastDatabaseOpener = Future<Database> Function({
  required String dbName,
  required int version,
  SembastCodec? codec,
  required MigrationCallback<Database> onVersionChanged,
});

/// A [StorageAdapter] backed by Sembast, on every platform.
///
/// **Dead-connection recovery.** After iOS Safari backgrounds or suspends a
/// tab, WebKit can leave the open IndexedDB connection unusable: the page
/// carries on, but every later read or write fails with an `UnknownError`
/// such as "Attempt to get an index record from database without an
/// in-progress transaction", until the page is reloaded. This adapter spots
/// that failure, closes and reopens the database (with the arguments of the
/// first [init]), and retries the operation once. Any other error, and a
/// second failure after the reopen, propagates unchanged.
///
/// Reopens run one at a time and never stack: operations failing together
/// share one, and an operation arriving mid-reopen waits for it. Recovery
/// gives up for the session when a reopen fails, has not finished within
/// [reopenTimeout], or is followed by another dead-connection failure on its
/// very first retry (the connection is not coming back, and reopening again
/// would only loop). From then on every error propagates at once. A
/// timed-out reopen is never started a second time, because it would queue
/// behind the same hung close and could open duplicate Sembast instances.
///
/// Accepted edge: a write that failed on the old connection is retried
/// after the reopen, so in a narrow race it can land after a newer write to
/// the same document. This is a cache; the next network read rewrites it.
class SembastAdapter implements StorageAdapter<Database> {
  Database? _db;
  final String dbName;

  /// How long an operation waits on a reopen of a dead connection before
  /// recovery gives up for the session. See the class doc.
  final Duration reopenTimeout;

  final SembastDatabaseOpener _openDatabase;

  // Namespaced under `__reliable_` so the reliable internal queue cannot
  // collide with an app collection of the same name. The original name was
  // `offline_queue`, which shared the underlying Sembast store with any
  // app collection by that name (the K type parameter is just a Dart-side
  // hint; the store identity is the name only). When the app stored
  // records there with String keys, every `_queueStore.find()` after that
  // threw `String is not a subtype of int` in Sembast's snapshot
  // deserialization, propagating up through `_mergeOfflineChanges` and
  // crashing every `fetch` with `networkOrElseCache`.
  final _queueStore = intMapStoreFactory.store('__reliable_action_queue');

  /// The arguments of the first [init], kept so a reopen opens the same
  /// database. Null until [init] has run.
  _OpenArguments? _openArguments;

  /// The reopen in flight, or null. Unbounded: the wait on it is bounded
  /// instead (see [_awaitReopen]), so a hung reopen is never started twice.
  Future<void>? _reopening;

  /// Set once recovery gives up (see the class doc). From then on every
  /// error propagates at once.
  var _gaveUp = false;

  /// Bumped when a reopen starts. An operation notes it before running. If
  /// it has moved by the time that operation fails, the failure came from
  /// the connection that was already replaced, and reopening again would
  /// close the healthy new one under every operation now using it.
  var _connection = 0;

  SembastAdapter({
    this.dbName = 'reliable_data.db',
    this.reopenTimeout = const Duration(seconds: 5),
    SembastDatabaseOpener? openDatabase,
  }) : _openDatabase = openDatabase ?? platform.openSembastDatabase;

  Database get db {
    if (_db == null) throw StateError('Database not initialized.');
    return _db!;
  }

  /// Whether dead-connection recovery has given up for this session. See
  /// the class doc.
  bool get recoveryGaveUp => _gaveUp;

  @override
  Future<void> init({
    ReliableEncryption? encryption,
    required int version,
    required MigrationCallback<Database> onUpgrade,
  }) async {
    SembastCodec? codec;
    if (encryption != null) {
      codec = SembastCodec(
        signature: encryption.signature,
        codec: _SembastWrapperCodec(encryption),
      );
    }
    final arguments = _OpenArguments(
      codec: codec,
      version: version,
      onUpgrade: onUpgrade,
    );
    _openArguments = arguments;
    _db = await _open(arguments);
  }

  Future<Database> _open(_OpenArguments arguments) {
    return _openDatabase(
      dbName: dbName,
      version: arguments.version,
      codec: arguments.codec,
      onVersionChanged: arguments.onUpgrade,
    );
  }

  // --- DEAD-CONNECTION RECOVERY ---

  Future<T> _withRecovery<T>(Future<T> Function() operation) async {
    // An operation arriving mid-reopen waits for it: the old database is
    // already closing, so running now would only fail with "database is
    // closed". Whatever the reopen's outcome, this one then just runs.
    await _awaitReopen();
    final connection = _connection;
    try {
      return await operation();
    } catch (error, stackTrace) {
      if (_gaveUp || !isDeadConnectionError(error)) rethrow;
      if (connection == _connection && _reopening == null) {
        _startReopen(error);
      }
      if (!await _awaitReopen()) {
        // The reopen failed or timed out. Report the original failure,
        // which is the one that explains what went wrong.
        Error.throwWithStackTrace(error, stackTrace);
      }
      try {
        return await operation();
      } catch (retryError) {
        if (isDeadConnectionError(retryError)) {
          _giveUp('still dead right after a reopen: $retryError');
        }
        rethrow;
      }
    }
  }

  void _startReopen(Object cause) {
    Log.alert('reliable: Sembast connection died ($cause) — reopening once');
    // Synchronous, before anything awaits, so an operation failing in the
    // same tick already sees the connection as replaced.
    _connection++;
    _reopening = _reopen().catchError((Object error) {
      _giveUp('reopen failed: $error');
    }).whenComplete(() => _reopening = null);
  }

  /// Waits for the reopen in flight, if any, for at most [reopenTimeout].
  /// Returns false when recovery has given up, now or earlier.
  Future<bool> _awaitReopen() async {
    final reopening = _reopening;
    if (reopening != null && !_gaveUp) {
      try {
        await reopening.timeout(reopenTimeout);
      } on TimeoutException {
        _giveUp('reopen still running after $reopenTimeout');
      }
    }
    return !_gaveUp;
  }

  void _giveUp(String reason) {
    if (_gaveUp) return;
    _gaveUp = true;
    Log.alert('reliable: Sembast recovery gave up for this session: $reason');
  }

  Future<void> _reopen() async {
    final arguments = _openArguments;
    if (arguments == null) {
      throw StateError('Cannot reopen: the database was never initialized.');
    }
    try {
      await _db?.close();
    } catch (_) {
      // The connection is already broken. Closing is only there to make
      // Sembast drop it, so a failure here changes nothing.
    }
    _db = await _open(arguments);
  }

  /// Whether [error] is how a dead database connection reads. The first two
  /// are WebKit's, raised as a generic DOM `UnknownError` /
  /// `InvalidStateError` with no more specific type to catch, so they are
  /// matched on the message. The third is Sembast's own, from an operation
  /// that ran against a database being closed by a reopen it raced.
  static bool isDeadConnectionError(Object error) {
    final message = '$error';
    return message.contains('without an in-progress transaction') ||
        message.contains('database connection is closing') ||
        message.contains('database is closed');
  }

  // --- CRUD ---
  @override
  Future<List<Map<String, dynamic>>> readCollection({
    required String collection,
  }) {
    return _withRecovery(() async {
      final store = stringMapStoreFactory.store(collection);
      final records = await store.find(db);
      return records.map((e) => Map<String, dynamic>.from(e.value)).toList();
    });
  }

  @override
  Future<void> writeDocument({
    required String collection,
    required String id,
    required Map<String, dynamic> data,
  }) {
    return _withRecovery(() async {
      await stringMapStoreFactory.store(collection).record(id).put(db, data);
    });
  }

  @override
  Future<void> deleteDocument({
    required String collection,
    required String id,
  }) {
    return _withRecovery(() async {
      await stringMapStoreFactory.store(collection).record(id).delete(db);
    });
  }

  @override
  Future<void> clearCollection({required String collection}) {
    return _withRecovery(() async {
      await stringMapStoreFactory.store(collection).delete(db);
    });
  }

  // --- QUEUE ---
  @override
  Future<void> queueAction(OfflineAction action) {
    return _withRecovery(() async {
      await _queueStore.add(db, action.toMap());
    });
  }

  @override
  Future<List<OfflineAction>> getQueue() {
    return _withRecovery(() async {
      final finder = Finder(
        sortOrders: [SortOrder(ReliableSchema.colTimestamp)],
      );
      final snapshots = await _queueStore.find(db, finder: finder);
      return snapshots.map((e) => OfflineAction.fromMap(e.value)).toList();
    });
  }

  @override
  Future<void> removeFromQueue(String uuid) {
    return _withRecovery(() async {
      await _queueStore.delete(
        db,
        finder: Finder(filter: Filter.equals(ReliableSchema.colUuid, uuid)),
      );
    });
  }

  @override
  Future<void> updateQueueItem(OfflineAction action) {
    return _withRecovery(() async {
      await _queueStore.update(
        db,
        action.toMap(),
        finder: Finder(
          filter: Filter.equals(ReliableSchema.colUuid, action.uuid),
        ),
      );
    });
  }

  // --- SECURITY ---
  @override
  Future<void> removeExpiredActions() {
    return _withRecovery(() async {
      final now = DateTime.now().millisecondsSinceEpoch;
      final finder = Finder(
        filter: Filter.and([
          Filter.notEquals(ReliableSchema.colExpiresAt, null),
          Filter.lessThan(ReliableSchema.colExpiresAt, now),
        ]),
      );
      await _queueStore.delete(db, finder: finder);
    });
  }

  @override
  Future<void> removeActionsForUser(String userId) {
    return _withRecovery(() async {
      await _queueStore.delete(
        db,
        finder: Finder(
          filter: Filter.equals(ReliableSchema.colUserId, userId),
        ),
      );
    });
  }
}

/// What [SembastAdapter.init] was called with, kept for a reopen.
final class _OpenArguments {
  const _OpenArguments({
    required this.codec,
    required this.version,
    required this.onUpgrade,
  });

  final SembastCodec? codec;
  final int version;
  final MigrationCallback<Database> onUpgrade;
}

// INTERNAL HELPERS
class _SembastWrapperCodec extends Codec<Object?, String> {
  final ReliableEncryption _impl;
  _SembastWrapperCodec(this._impl);

  @override
  Converter<String, Object?> get decoder => _SembastDecoder(_impl);
  @override
  Converter<Object?, String> get encoder => _SembastEncoder(_impl);
}

class _SembastEncoder extends Converter<Object?, String> {
  final ReliableEncryption _impl;
  _SembastEncoder(this._impl);

  @override
  String convert(Object? input) {
    final jsonString = json.encode(input);
    return _impl.encrypt(jsonString);
  }
}

class _SembastDecoder extends Converter<String, Object?> {
  final ReliableEncryption _impl;
  _SembastDecoder(this._impl);

  @override
  Object? convert(String input) {
    final decrypted = _impl.decrypt(input);
    return json.decode(decrypted);
  }
}

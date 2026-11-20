import 'dart:async';

import 'package:df_log/df_log.dart';
import 'package:synchronized/synchronized.dart';
import 'package:uuid/uuid.dart';

import '../reliable_core.dart';
import 'sync_mutation.dart';

/// One page of what changed on the server since a cursor, as the app's
/// [SyncServer] reads it off the wire.
class SyncPage {
  const SyncPage({
    required this.isSnapshot,
    required this.upserts,
    required this.removals,
    required this.cursor,
    required this.hasMore,
  });

  /// True when [upserts] is everything the device should hold: the replica
  /// is replaced by it. A snapshot is always one page.
  final bool isSnapshot;

  /// Current documents by collection; each carries its id under the
  /// engine's `idField`.
  final Map<String, List<Map<String, dynamic>>> upserts;

  /// Ids the device must forget, by collection (deleted, or no longer
  /// visible to it). Always empty on a snapshot.
  final Map<String, List<String>> removals;

  /// Opaque; handed back on the next pull.
  final String cursor;

  /// True when more changes are waiting: the engine pulls again at once.
  final bool hasMore;
}

/// The server side of a pull: "what changed since [cursor]?" (null on a
/// device that has never pulled). Throws [ReliableNetworkException] when no
/// answer came.
abstract interface class SyncServer {
  Future<SyncPage> pull(String? cursor);
}

/// The documents of the view while mutations are applied to them.
class SyncViewDraft {
  SyncViewDraft._(this._collections);

  final Map<String, Map<String, Map<String, dynamic>>> _collections;

  /// The documents of [collection] by id, to replace, add or remove. A
  /// document itself is read-only (it is shared with the replica): put a
  /// changed copy back under its id.
  Map<String, Map<String, dynamic>> docs(String collection) =>
      _collections[collection] ??
      (throw StateError('"$collection" is not a collection this engine syncs'));
}

/// One kind of change the device can make: how it shows locally at once,
/// and how it reaches the server.
abstract interface class SyncMutationKind {
  /// Stored with each mutation; must stay stable across app versions.
  String get name;

  /// Applies [mutation] to [draft], as the server will once it has it. Runs
  /// on every view rebuild until a pull confirms it, so it must be pure.
  void apply(SyncMutation mutation, SyncViewDraft draft);

  /// Sends [mutation], with `mutation.id` as the idempotency key. Throws
  /// [ReliableNetworkException]: with a 4xx status (but 408 or 429) the
  /// server refused it for good and it is dropped; with no status or any
  /// other, it is retried on a later flush ([SyncEngine.retryDelay]).
  /// [SyncMutation.isResend] says whether an earlier send may already have
  /// reached the server.
  Future<void> send(SyncMutation mutation);
}

/// A local-first sync engine over any [StorageAdapter].
///
/// The device holds a **replica** (the server's rows as of the last pull,
/// per collection) and an **outbox** (its own changes, in order). What the
/// app shows is the **view**: the replica with every outbox mutation applied
/// on top, rebuilt whenever either changes. So a change shows at once,
/// survives a restart, and when the server's answer lands the view is
/// "server truth plus what it has not seen yet", never a guess.
///
/// - [mutate] stores a change, shows it, and is all a screen calls.
/// - [flush] sends the outbox in order; a send with no answer or a server
///   error stops it (retried later, same idempotency key, after
///   [retryDelay]), a refusal drops that one change ([onRefused]) and goes
///   on. After anything was accepted it pulls.
/// - [pull] reads pages from [server] until none are left. A sent change
///   leaves the outbox only when a pull that started after its acceptance
///   lands, so it never blinks out of the view.
///
/// Storage stays document-only: the replica is one collection per synced
/// collection, the outbox and the cursor are documents too, each in a
/// storage collection named under [namespace] ([replicaStoreName],
/// [outboxStoreName], [metaStoreName]).
///
/// [close] lets go of the stores at once: a send or pull still under way
/// when it is called finishes, but nothing it learns is written. So the
/// stores can be handed to someone else (another account signing in on the
/// device) the moment [close] returns.
class SyncEngine {
  SyncEngine({
    required StorageAdapter<dynamic> storage,
    required SyncServer server,
    required List<SyncMutationKind> kinds,
    required Set<String> collections,
    this.namespace = 'SYNC',
    this.idField = 'id',
    this.onRefused,
    this.firstRetryDelay = const Duration(seconds: 2),
    this.maxRetryDelay = const Duration(minutes: 5),
    this.reportFailuresAfter = 10,
  })  : _storage = storage,
        _server = server,
        _kinds = {for (final kind in kinds) kind.name: kind},
        _collections = collections;

  final StorageAdapter<dynamic> _storage;
  final SyncServer _server;
  final Map<String, SyncMutationKind> _kinds;
  final Set<String> _collections;

  /// Prefix of every storage collection the engine writes (see
  /// [replicaStoreName], [outboxStoreName] and [metaStoreName]).
  final String namespace;

  /// The key each document's id is stored under.
  final String idField;

  /// Called when the server refused a change for good (it is already gone
  /// from the outbox and the view): the place to tell the user.
  final void Function(SyncMutation mutation, ReliableNetworkException error)?
      onRefused;

  /// How long [retryDelay] waits after a change's first failed send; each
  /// further failed send doubles it, up to [maxRetryDelay].
  final Duration firstRetryDelay;

  /// The longest [retryDelay] gets.
  final Duration maxRetryDelay;

  /// When the server answers a send with an error (not a missing answer,
  /// which is the connection's fault) and the change has been sent this many
  /// times, the failure is logged as an error, once per engine: a change the
  /// server keeps failing holds up every change behind it, so someone must
  /// look.
  final int reportFailuresAfter;

  final _replica = <String, Map<String, Map<String, dynamic>>>{};
  final _outbox = <SyncMutation>[];
  var _view = <String, Map<String, Map<String, dynamic>>>{};
  String? _cursor;
  var _nextSeq = 1;
  var _isOpen = false;
  var _isClosed = false;
  final _pullLock = Lock();
  final _flushLock = Lock();
  final _changes = StreamController<void>.broadcast();

  /// The id of the mutation [flush] is sending right now, if any: [discard]
  /// leaves it alone, as the server may already have it.
  String? _sendingId;

  /// Changes whose failing sends were already logged ([reportFailuresAfter]).
  final _reportedFailures = <String>{};

  static const _kCursorDocId = 'cursor';
  static const _kCursorValue = 'value';

  /// The storage collection holding the replica of [collection]:
  /// `<namespace>_REPLICA_<collection>`.
  String replicaStoreName(String collection) =>
      '${namespace}_REPLICA_$collection';

  /// The storage collection holding the outbox: `<namespace>_OUTBOX`.
  String get outboxStoreName => '${namespace}_OUTBOX';

  /// The storage collection holding the cursor: `<namespace>_META`.
  String get metaStoreName => '${namespace}_META';

  /// Fires after every change to the view.
  Stream<void> get changes => _changes.stream;

  /// Changes the server has not confirmed yet (sent or not).
  int get pendingCount => _outbox.length;

  /// The changes the server has not confirmed yet, oldest first.
  List<SyncMutation> get pending => List.unmodifiable(_outbox);

  /// True once a pull has landed (or a stored cursor says one did).
  bool get hasPulled => _cursor != null;

  /// How long to wait before the next [flush] is worth trying: null when no
  /// unsent change is waiting, zero when the oldest one was never sent, and
  /// otherwise [firstRetryDelay] doubled for each further failed send of it,
  /// up to [maxRetryDelay]. The engine never flushes by itself: its owner
  /// schedules the retry (and flushes at once on a reconnect).
  Duration? get retryDelay {
    final oldest = _outbox.where((m) => !m.isSent).firstOrNull;
    if (oldest == null) return null;
    if (oldest.attempts == 0) return Duration.zero;
    final doublings = oldest.attempts - 1;
    // Past 20 doublings any first delay is beyond every sane maximum.
    if (doublings > 20) return maxRetryDelay;
    final delay = firstRetryDelay * (1 << doublings);
    return delay < maxRetryDelay ? delay : maxRetryDelay;
  }

  /// The view of [collection]: documents by id, read-only.
  Map<String, Map<String, dynamic>> view(String collection) {
    _requireOpen();
    _throwIfClosed();
    final docs = _view[collection];
    if (docs == null) {
      throw StateError('"$collection" is not a collection this engine syncs');
    }
    return Map.unmodifiable(docs);
  }

  /// Loads the replica, the outbox and the cursor from storage. Call once,
  /// before anything else.
  Future<void> open() async {
    for (final collection in _collections) {
      final docs = await _storage.readCollection(
        collection: replicaStoreName(collection),
      );
      _replica[collection] = {
        for (final doc in docs) _idOf(doc, collection): Map.unmodifiable(doc),
      };
    }
    final stored = await _storage.readCollection(collection: outboxStoreName);
    for (final raw in stored) {
      try {
        _outbox.add(SyncMutation.fromMap(raw));
      } on FormatException catch (e) {
        // One corrupt row must not stall every change behind it for good.
        Log.err('Dropped an unreadable sync mutation: $e');
        final id = raw['id'];
        if (id is String) {
          await _storage.deleteDocument(collection: outboxStoreName, id: id);
        }
      }
    }
    _outbox.sort((a, b) => a.seq.compareTo(b.seq));
    _nextSeq = _outbox.isEmpty ? 1 : _outbox.last.seq + 1;
    final meta = await _storage.readCollection(collection: metaStoreName);
    for (final doc in meta) {
      if (doc[idField] == _kCursorDocId) _cursor = doc[_kCursorValue] as String;
    }
    _isOpen = true;
    _rebuildView();
  }

  /// Records a change of [kind] and shows it at once. [id] is the
  /// mutation's id (and idempotency key); one is minted when omitted.
  Future<SyncMutation> mutate(
    String kind,
    Map<String, dynamic> payload, {
    String? id,
  }) async {
    _requireOpen();
    _throwIfClosed();
    if (!_kinds.containsKey(kind)) {
      throw ArgumentError.value(
        kind,
        'kind',
        'no SyncMutationKind has this name',
      );
    }
    final mutation = SyncMutation(
      id: id ?? const Uuid().v4(),
      seq: _nextSeq++,
      kind: kind,
      payload: payload,
      createdAt: DateTime.now().toUtc(),
      attempts: 0,
      isSent: false,
    );
    // Stored before it shows: a change on screen survives a restart.
    await _storeMutation(mutation);
    _outbox.add(mutation);
    _rebuildView();
    return mutation;
  }

  /// Sends unsent changes in order, then pulls if any was accepted. Does
  /// nothing once the engine is closed.
  Future<void> flush() async {
    if (_isClosed) return;
    _requireOpen();
    final anyAccepted = await _whileOpen(
      () => _flushLock.synchronized(() async {
        var accepted = false;
        for (final queued in List.of(_outbox)) {
          if (queued.isSent) continue;
          // Discarded while an earlier one was being sent.
          if (!_outbox.any((m) => m.id == queued.id)) continue;
          final kind = _kinds[queued.kind];
          if (kind == null) {
            // Stored by a build that knew this kind: nothing here can send
            // it.
            Log.err('Dropped a sync mutation of unknown kind "${queued.kind}"');
            await _dropMutation(queued);
            continue;
          }
          // Claimed before anything is awaited, so [discard] leaves it be.
          _sendingId = queued.id;
          // Counted before it goes: if the answer is lost (or the app is
          // closed mid-send), the next send knows the server may have it.
          final mutation = queued.copyWith(attempts: queued.attempts + 1);
          try {
            await _replaceMutation(mutation);
            await kind.send(mutation);
          } on _EngineClosed {
            rethrow;
          } on ReliableNetworkException catch (e) {
            if (_isRefusal(e)) {
              await _dropMutation(mutation);
              onRefused?.call(mutation, e);
              continue;
            }
            // No answer, or the server failed: keep it, in order, for later.
            _reportIfFailingForGood(mutation, e);
            return accepted;
          } catch (e, stackTrace) {
            // A bug in the kind's send (or a broken store): keep the change
            // and stop, loudly.
            Log.err(
              'Sending sync mutation ${mutation.kind} failed: $e\n$stackTrace',
            );
            return accepted;
          } finally {
            _sendingId = null;
          }
          await _replaceMutation(mutation.copyWith(isSent: true));
          accepted = true;
        }
        return accepted;
      }),
      whenClosed: false,
    );
    if (anyAccepted) await pull();
  }

  /// Logs, once, that the server keeps failing [mutation] ([e] is the
  /// latest failure; see [reportFailuresAfter]).
  void _reportIfFailingForGood(
    SyncMutation mutation,
    ReliableNetworkException e,
  ) {
    if (e.statusCode == null) return;
    if (mutation.attempts < reportFailuresAfter) return;
    if (!_reportedFailures.add(mutation.id)) return;
    Log.err(
      'The server failed sync mutation ${mutation.kind} ${mutation.id} '
      '${mutation.attempts} times (last: $e); it holds up '
      '${_outbox.where((m) => !m.isSent).length - 1} change(s) behind it',
    );
  }

  /// Takes back a change the server has not seen: removes the unsent
  /// mutation [id] from the outbox and the view, as if it was never made.
  /// Returns false, changing nothing, when there is no such mutation or it
  /// was already sent (or is being sent): the server may have it, so only a
  /// new mutation can undo it.
  ///
  /// For an undo made before the change left the device (a tick taken back
  /// offline), so the server never hears of either.
  Future<bool> discard(String id) async {
    _requireOpen();
    _throwIfClosed();
    final index = _outbox.indexWhere((m) => m.id == id);
    if (index < 0) return false;
    final mutation = _outbox[index];
    if (mutation.isSent || mutation.id == _sendingId) return false;
    await _dropMutation(mutation);
    return true;
  }

  /// Pulls every waiting page from the server into the replica. Does
  /// nothing once the engine is closed.
  Future<void> pull() async {
    if (_isClosed) return;
    _requireOpen();
    await _whileOpen(
      () => _pullLock.synchronized(() async {
        final confirmedByThisPull = {
          for (final mutation in _outbox)
            if (mutation.isSent) mutation.id,
        };
        while (true) {
          _throwIfClosed();
          final page = await _server.pull(_cursor);
          if (page.isSnapshot && page.hasMore) {
            throw const FormatException('A snapshot must be a single page');
          }
          await _applyPage(page);
          if (!page.hasMore) break;
        }
        for (final mutation in List.of(_outbox)) {
          if (confirmedByThisPull.contains(mutation.id)) {
            await _store.deleteDocument(
              collection: outboxStoreName,
              id: mutation.id,
            );
            _outbox.remove(mutation);
          }
        }
        _rebuildView();
      }),
      whenClosed: null,
    );
  }

  /// Forgets everything stored: replica, outbox and cursor (sign-out).
  /// Not on a closed engine: the stores may be someone else's by then.
  Future<void> clear() async {
    _throwIfClosed();
    await _pullLock.synchronized(() async {
      await _flushLock.synchronized(() async {
        for (final collection in _collections) {
          await _storage.clearCollection(
            collection: replicaStoreName(collection),
          );
          _replica[collection] = {};
        }
        await _storage.clearCollection(collection: outboxStoreName);
        await _storage.clearCollection(collection: metaStoreName);
        _outbox.clear();
        _cursor = null;
        _nextSeq = 1;
        if (_isOpen) _rebuildView();
      });
    });
  }

  /// Lets go of the stores: from now on nothing is written to them. A send
  /// or pull still under way finishes, and what it learns is dropped (a
  /// change it sent stays in the outbox as it was, to be sent again, under
  /// the same idempotency key, by whoever opens these stores next as the
  /// same user). [flush] and [pull] then do nothing; the rest throw.
  Future<void> close() {
    _isClosed = true;
    return _changes.close();
  }

  /// Runs [body], which ends early by throwing [_EngineClosed] when the
  /// engine is closed under it; [whenClosed] is then the result.
  Future<T> _whileOpen<T>(
    Future<T> Function() body, {
    required T whenClosed,
  }) async {
    try {
      return await body();
    } on _EngineClosed {
      return whenClosed;
    }
  }

  /// The stores, while the engine is not closed. Every write goes through
  /// here, so none lands after [close].
  StorageAdapter<dynamic> get _store {
    _throwIfClosed();
    return _storage;
  }

  void _throwIfClosed() {
    if (_isClosed) throw _EngineClosed();
  }

  Future<void> _applyPage(SyncPage page) async {
    for (final collection in [...page.upserts.keys, ...page.removals.keys]) {
      if (!_collections.contains(collection)) {
        throw FormatException(
          'A pull named "$collection", which this engine does not sync',
        );
      }
    }
    if (page.isSnapshot) {
      // The cursor goes first: a snapshot cut short by a crash leaves no
      // cursor, so the next pull is a snapshot again.
      await _store.deleteDocument(
        collection: metaStoreName,
        id: _kCursorDocId,
      );
      for (final collection in _collections) {
        await _store.clearCollection(
          collection: replicaStoreName(collection),
        );
        _replica[collection] = {};
      }
    }
    for (final entry in page.upserts.entries) {
      final docs = _replica[entry.key]!;
      for (final doc in entry.value) {
        final id = _idOf(doc, entry.key);
        await _store.writeDocument(
          collection: replicaStoreName(entry.key),
          id: id,
          data: doc,
        );
        docs[id] = Map.unmodifiable(doc);
      }
    }
    for (final entry in page.removals.entries) {
      final docs = _replica[entry.key]!;
      for (final id in entry.value) {
        await _store.deleteDocument(
          collection: replicaStoreName(entry.key),
          id: id,
        );
        docs.remove(id);
      }
    }
    await _store.writeDocument(
      collection: metaStoreName,
      id: _kCursorDocId,
      data: {idField: _kCursorDocId, _kCursorValue: page.cursor},
    );
    _cursor = page.cursor;
  }

  void _rebuildView() {
    final collections = {
      for (final entry in _replica.entries) entry.key: Map.of(entry.value),
    };
    final draft = SyncViewDraft._(collections);
    for (final mutation in _outbox) {
      final kind = _kinds[mutation.kind];
      if (kind == null) continue; // flush drops it, loudly
      try {
        kind.apply(mutation, draft);
      } catch (e, stackTrace) {
        // A bug in one kind's apply must not blank the whole view.
        Log.err(
          'Applying sync mutation ${mutation.kind} failed: $e\n$stackTrace',
        );
      }
    }
    _view = collections;
    if (!_changes.isClosed) _changes.add(null);
  }

  Future<void> _storeMutation(SyncMutation mutation) => _store.writeDocument(
        collection: outboxStoreName,
        id: mutation.id,
        data: mutation.toMap(),
      );

  Future<void> _replaceMutation(SyncMutation mutation) async {
    await _storeMutation(mutation);
    final index = _outbox.indexWhere((m) => m.id == mutation.id);
    if (index >= 0) _outbox[index] = mutation;
  }

  Future<void> _dropMutation(SyncMutation mutation) async {
    await _store.deleteDocument(
      collection: outboxStoreName,
      id: mutation.id,
    );
    _outbox.removeWhere((m) => m.id == mutation.id);
    _rebuildView();
  }

  String _idOf(Map<String, dynamic> doc, String collection) {
    final id = doc[idField];
    if (id is String && id.isNotEmpty) return id;
    throw FormatException(
      'A "$collection" document has no string "$idField": $doc',
    );
  }

  void _requireOpen() {
    if (!_isOpen) throw StateError('SyncEngine.open() has not run');
  }

  static bool _isRefusal(ReliableNetworkException e) {
    final status = e.statusCode;
    return status != null &&
        status >= 400 &&
        status < 500 &&
        status != 408 &&
        status != 429;
  }
}

/// The engine was used after [SyncEngine.close]. A flush or pull it ends
/// is caught, so that one just stops; anything else (a change recorded as
/// the engine closes) gets it as the [StateError] it is.
class _EngineClosed extends StateError {
  _EngineClosed() : super('SyncEngine is closed');
}

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
  /// other, it is retried on the next flush.
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
/// - [flush] sends the outbox in order; a send with no answer stops it
///   (retried later, same idempotency key), a refusal drops that one change
///   ([onRefused]) and goes on. After anything was accepted it pulls.
/// - [pull] reads pages from [server] until none are left. A sent change
///   leaves the outbox only when a pull that started after its acceptance
///   lands, so it never blinks out of the view.
///
/// Storage stays document-only: the replica is one collection per synced
/// collection, the outbox and the cursor are documents too, all under
/// [namespace].
class SyncEngine {
  SyncEngine({
    required StorageAdapter<dynamic> storage,
    required SyncServer server,
    required List<SyncMutationKind> kinds,
    required Set<String> collections,
    this.namespace = 'sync',
    this.idField = 'id',
    this.onRefused,
  })  : _storage = storage,
        _server = server,
        _kinds = {for (final kind in kinds) kind.name: kind},
        _collections = collections;

  final StorageAdapter<dynamic> _storage;
  final SyncServer _server;
  final Map<String, SyncMutationKind> _kinds;
  final Set<String> _collections;

  /// Prefix of every storage collection the engine writes.
  final String namespace;

  /// The key each document's id is stored under.
  final String idField;

  /// Called when the server refused a change for good (it is already gone
  /// from the outbox and the view): the place to tell the user.
  final void Function(SyncMutation mutation, ReliableNetworkException error)?
      onRefused;

  final _replica = <String, Map<String, Map<String, dynamic>>>{};
  final _outbox = <SyncMutation>[];
  var _view = <String, Map<String, Map<String, dynamic>>>{};
  String? _cursor;
  var _nextSeq = 1;
  var _isOpen = false;
  final _pullLock = Lock();
  final _flushLock = Lock();
  final _changes = StreamController<void>.broadcast();

  static const _kCursorDocId = 'cursor';
  static const _kCursorValue = 'value';

  String _replicaCollection(String collection) =>
      '${namespace}__replica__$collection';
  String get _outboxCollection => '${namespace}__outbox';
  String get _metaCollection => '${namespace}__meta';

  /// Fires after every change to the view.
  Stream<void> get changes => _changes.stream;

  /// Changes the server has not confirmed yet (sent or not).
  int get pendingCount => _outbox.length;

  /// The changes the server has not confirmed yet, oldest first.
  List<SyncMutation> get pending => List.unmodifiable(_outbox);

  /// True once a pull has landed (or a stored cursor says one did).
  bool get hasPulled => _cursor != null;

  /// The view of [collection]: documents by id, read-only.
  Map<String, Map<String, dynamic>> view(String collection) {
    _requireOpen();
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
        collection: _replicaCollection(collection),
      );
      _replica[collection] = {
        for (final doc in docs) _idOf(doc, collection): Map.unmodifiable(doc),
      };
    }
    final stored = await _storage.readCollection(collection: _outboxCollection);
    for (final raw in stored) {
      try {
        _outbox.add(SyncMutation.fromMap(raw));
      } on FormatException catch (e) {
        // One corrupt row must not stall every change behind it for good.
        Log.err('Dropped an unreadable sync mutation: $e');
        final id = raw['id'];
        if (id is String) {
          await _storage.deleteDocument(collection: _outboxCollection, id: id);
        }
      }
    }
    _outbox.sort((a, b) => a.seq.compareTo(b.seq));
    _nextSeq = _outbox.isEmpty ? 1 : _outbox.last.seq + 1;
    final meta = await _storage.readCollection(collection: _metaCollection);
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

  /// Sends unsent changes in order, then pulls if any was accepted.
  Future<void> flush() async {
    _requireOpen();
    final anyAccepted = await _flushLock.synchronized(() async {
      var accepted = false;
      for (final mutation in List.of(_outbox)) {
        if (mutation.isSent) continue;
        final kind = _kinds[mutation.kind];
        if (kind == null) {
          // Stored by a build that knew this kind: nothing here can send it.
          Log.err('Dropped a sync mutation of unknown kind "${mutation.kind}"');
          await _dropMutation(mutation);
          continue;
        }
        try {
          await kind.send(mutation);
        } on ReliableNetworkException catch (e) {
          if (_isRefusal(e)) {
            await _dropMutation(mutation);
            onRefused?.call(mutation, e);
            continue;
          }
          // No answer, or the server failed: keep it, in order, for later.
          await _replaceMutation(
            mutation.copyWith(attempts: mutation.attempts + 1),
          );
          return accepted;
        } catch (e, stackTrace) {
          // A bug in the kind's send: keep the change and stop, loudly.
          Log.err(
            'Sending sync mutation ${mutation.kind} failed: $e\n$stackTrace',
          );
          return accepted;
        }
        await _replaceMutation(mutation.copyWith(isSent: true));
        accepted = true;
      }
      return accepted;
    });
    if (anyAccepted) await pull();
  }

  /// Pulls every waiting page from the server into the replica.
  Future<void> pull() async {
    _requireOpen();
    await _pullLock.synchronized(() async {
      final confirmedByThisPull = {
        for (final mutation in _outbox)
          if (mutation.isSent) mutation.id,
      };
      while (true) {
        final page = await _server.pull(_cursor);
        if (page.isSnapshot && page.hasMore) {
          throw const FormatException('A snapshot must be a single page');
        }
        await _applyPage(page);
        if (!page.hasMore) break;
      }
      for (final mutation in List.of(_outbox)) {
        if (confirmedByThisPull.contains(mutation.id)) {
          await _storage.deleteDocument(
            collection: _outboxCollection,
            id: mutation.id,
          );
          _outbox.remove(mutation);
        }
      }
      _rebuildView();
    });
  }

  /// Forgets everything stored: replica, outbox and cursor (sign-out).
  Future<void> clear() async {
    await _pullLock.synchronized(() async {
      await _flushLock.synchronized(() async {
        for (final collection in _collections) {
          await _storage.clearCollection(
            collection: _replicaCollection(collection),
          );
          _replica[collection] = {};
        }
        await _storage.clearCollection(collection: _outboxCollection);
        await _storage.clearCollection(collection: _metaCollection);
        _outbox.clear();
        _cursor = null;
        _nextSeq = 1;
        if (_isOpen) _rebuildView();
      });
    });
  }

  Future<void> close() => _changes.close();

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
      await _storage.deleteDocument(
        collection: _metaCollection,
        id: _kCursorDocId,
      );
      for (final collection in _collections) {
        await _storage.clearCollection(
          collection: _replicaCollection(collection),
        );
        _replica[collection] = {};
      }
    }
    for (final entry in page.upserts.entries) {
      final docs = _replica[entry.key]!;
      for (final doc in entry.value) {
        final id = _idOf(doc, entry.key);
        await _storage.writeDocument(
          collection: _replicaCollection(entry.key),
          id: id,
          data: doc,
        );
        docs[id] = Map.unmodifiable(doc);
      }
    }
    for (final entry in page.removals.entries) {
      final docs = _replica[entry.key]!;
      for (final id in entry.value) {
        await _storage.deleteDocument(
          collection: _replicaCollection(entry.key),
          id: id,
        );
        docs.remove(id);
      }
    }
    await _storage.writeDocument(
      collection: _metaCollection,
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

  Future<void> _storeMutation(SyncMutation mutation) => _storage.writeDocument(
        collection: _outboxCollection,
        id: mutation.id,
        data: mutation.toMap(),
      );

  Future<void> _replaceMutation(SyncMutation mutation) async {
    await _storeMutation(mutation);
    final index = _outbox.indexWhere((m) => m.id == mutation.id);
    if (index >= 0) _outbox[index] = mutation;
  }

  Future<void> _dropMutation(SyncMutation mutation) async {
    await _storage.deleteDocument(
      collection: _outboxCollection,
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

// SyncEngine against a fake server (an in-memory table with a change log)
// over a real in-memory Sembast store: changes show at once, survive a
// restart, reach the server in order, replay safely, and the view converges
// on the server's truth.

import 'package:flutter_test/flutter_test.dart';
import 'package:reliable/reliable.dart';
import 'package:sembast/sembast_memory.dart';

/// A server with one collection, `notes`, and a change log the pull reads.
class _FakeServer implements SyncServer {
  final notes = <String, Map<String, dynamic>>{};
  final _log = <String>[]; // note ids, in change order
  final appliedMutationIds = <String>{};

  /// When true, every request fails as if the device were offline.
  var isOffline = false;

  /// When true, a send is carried out and then its answer is lost.
  var losesAnswers = false;

  /// Names (note text) the server refuses with a 422.
  final refusedNames = <String>{};

  var pageSize = 100;

  void write(String id, String text) {
    notes[id] = {'id': id, 'text': text};
    _log.add(id);
  }

  void delete(String id) {
    notes.remove(id);
    _log.add(id);
  }

  /// What the `PUT_NOTE` / `DELETE_NOTE` kinds call.
  Future<void> receive(SyncMutation mutation) async {
    if (isOffline) throw const ReliableNetworkException(message: 'offline');
    // Idempotency: a replay of an applied mutation changes nothing.
    if (appliedMutationIds.add(mutation.id)) {
      final text = mutation.payload['text'] as String?;
      if (text != null && refusedNames.contains(text)) {
        appliedMutationIds.remove(mutation.id);
        throw const ReliableNetworkException(
          message: 'refused',
          statusCode: 422,
        );
      }
      final id = mutation.payload['id'] as String;
      if (mutation.kind == 'PUT_NOTE') {
        write(id, text!);
      } else {
        delete(id);
      }
    }
    if (losesAnswers) {
      throw const ReliableNetworkException(message: 'answer lost');
    }
  }

  @override
  Future<SyncPage> pull(String? cursor) async {
    if (isOffline) throw const ReliableNetworkException(message: 'offline');
    if (cursor == null) {
      return SyncPage(
        isSnapshot: true,
        upserts: {'notes': notes.values.toList()},
        removals: const {},
        cursor: '${_log.length}',
        hasMore: false,
      );
    }
    final from = int.parse(cursor);
    final to = (from + pageSize).clamp(0, _log.length);
    final ids = _log.sublist(from, to).toSet();
    return SyncPage(
      isSnapshot: false,
      upserts: {
        'notes': [
          for (final id in ids)
            if (notes[id] != null) notes[id]!,
        ],
      },
      removals: {
        'notes': [
          for (final id in ids)
            if (notes[id] == null) id,
        ],
      },
      cursor: '$to',
      hasMore: to < _log.length,
    );
  }
}

class _PutNote implements SyncMutationKind {
  _PutNote(this.server);
  final _FakeServer server;

  @override
  String get name => 'PUT_NOTE';

  @override
  void apply(SyncMutation mutation, SyncViewDraft draft) {
    final id = mutation.payload['id'] as String;
    draft.docs('notes')[id] = {'id': id, 'text': mutation.payload['text']};
  }

  @override
  Future<void> send(SyncMutation mutation) => server.receive(mutation);
}

class _DeleteNote implements SyncMutationKind {
  _DeleteNote(this.server);
  final _FakeServer server;

  @override
  String get name => 'DELETE_NOTE';

  @override
  void apply(SyncMutation mutation, SyncViewDraft draft) {
    draft.docs('notes').remove(mutation.payload['id']);
  }

  @override
  Future<void> send(SyncMutation mutation) => server.receive(mutation);
}

Future<SembastAdapter> _openStorage(String name) async {
  final storage = SembastAdapter(
    dbName: name,
    openDatabase: ({
      required dbName,
      required version,
      codec,
      required onVersionChanged,
    }) =>
        databaseFactoryMemory.openDatabase(
      dbName,
      version: version,
      codec: codec,
    ),
  );
  await storage.init(
    version: 1,
    onUpgrade: (db, oldVersion, newVersion) async {},
  );
  return storage;
}

void main() {
  late _FakeServer server;
  late String dbName;
  final refused = <SyncMutation>[];

  Future<SyncEngine> openEngine() async {
    final engine = SyncEngine(
      storage: await _openStorage(dbName),
      server: server,
      kinds: [_PutNote(server), _DeleteNote(server)],
      collections: {'notes'},
      onRefused: (mutation, _) => refused.add(mutation),
    );
    await engine.open();
    return engine;
  }

  Map<String, String> texts(SyncEngine engine) => {
        for (final doc in engine.view('notes').values)
          doc['id'] as String: doc['text'] as String,
      };

  setUp(() {
    server = _FakeServer();
    dbName = 'sync_${DateTime.now().microsecondsSinceEpoch}.db';
    refused.clear();
  });

  test('a first pull is a snapshot of the server', () async {
    server.write('a', 'Buy milk');
    final engine = await openEngine();
    expect(engine.hasPulled, isFalse);
    await engine.pull();
    expect(texts(engine), {'a': 'Buy milk'});
    expect(engine.hasPulled, isTrue);
  });

  test('an offline change shows at once and survives a restart', () async {
    final engine = await openEngine();
    server.isOffline = true;
    await engine.mutate('PUT_NOTE', {'id': 'a', 'text': 'Offline note'});
    expect(texts(engine), {'a': 'Offline note'});
    await engine.flush(); // no answer: kept
    expect(engine.pendingCount, 1);

    final restarted = await openEngine();
    expect(texts(restarted), {'a': 'Offline note'});
    expect(restarted.pendingCount, 1);
    server.isOffline = false;
    await restarted.flush();
    expect(server.notes.keys, ['a']);
    expect(restarted.pendingCount, 0);
    expect(texts(restarted), {'a': 'Offline note'});
  });

  test('changes reach the server in the order they were made', () async {
    final engine = await openEngine();
    server.isOffline = true;
    await engine.mutate('PUT_NOTE', {'id': 'a', 'text': 'First'});
    await engine.mutate('PUT_NOTE', {'id': 'a', 'text': 'Second'});
    await engine.mutate('DELETE_NOTE', {'id': 'a'});
    await engine.mutate('PUT_NOTE', {'id': 'b', 'text': 'Kept'});
    expect(texts(engine), {'b': 'Kept'});
    server.isOffline = false;
    await engine.flush();
    expect(server.notes, {
      'b': {'id': 'b', 'text': 'Kept'},
    });
    expect(texts(engine), {'b': 'Kept'});
  });

  test('a lost answer replays under the same id and applies once', () async {
    final engine = await openEngine();
    server.losesAnswers = true;
    await engine.mutate('PUT_NOTE', {'id': 'a', 'text': 'Once'});
    await engine.flush();
    expect(engine.pendingCount, 1);
    server.losesAnswers = false;
    await engine.flush();
    expect(server.appliedMutationIds, hasLength(1));
    expect(engine.pendingCount, 0);
    expect(texts(engine), {'a': 'Once'});
  });

  test('a refused change is dropped, reported, and the rest still go',
      () async {
    final engine = await openEngine();
    server.refusedNames.add('Rude');
    await engine.mutate('PUT_NOTE', {'id': 'a', 'text': 'Rude'});
    await engine.mutate('PUT_NOTE', {'id': 'b', 'text': 'Fine'});
    await engine.flush();
    expect(refused.map((m) => m.payload['text']), ['Rude']);
    expect(texts(engine), {'b': 'Fine'});
  });

  test('another device’s changes arrive with the next pull, paged', () async {
    final engine = await openEngine();
    await engine.pull();
    server.pageSize = 2;
    for (var i = 0; i < 5; i++) {
      server.write('n$i', 'Note $i');
    }
    server.delete('n0');
    await engine.pull();
    expect(texts(engine).keys.toSet(), {'n1', 'n2', 'n3', 'n4'});
  });

  test('a local change wins in the view until the server has it', () async {
    server.write('a', 'Server text');
    final engine = await openEngine();
    await engine.pull();
    server.isOffline = true;
    await engine.mutate('PUT_NOTE', {'id': 'a', 'text': 'My edit'});
    server.isOffline = false;
    server.write('a', 'Someone else’s edit'); // lands before mine
    await engine.pull();
    expect(texts(engine), {'a': 'My edit'}, reason: 'rebased on the new truth');
    await engine.flush();
    expect(
      texts(engine),
      {'a': 'My edit'},
      reason: 'the server order: mine came last',
    );
  });

  test('clear forgets the replica, the outbox and the cursor', () async {
    server.write('a', 'x');
    final engine = await openEngine();
    await engine.pull();
    await engine.mutate('PUT_NOTE', {'id': 'b', 'text': 'y'});
    await engine.clear();
    expect(texts(engine), isEmpty);
    expect(engine.pendingCount, 0);
    expect(engine.hasPulled, isFalse);
  });

  test('a page naming a collection the engine does not sync is an error',
      () async {
    final engine = SyncEngine(
      storage: await _openStorage(dbName),
      server: _UnknownCollectionServer(),
      kinds: const [],
      collections: {'notes'},
    );
    await engine.open();
    await expectLater(engine.pull(), throwsFormatException);
  });
}

class _UnknownCollectionServer implements SyncServer {
  @override
  Future<SyncPage> pull(String? cursor) async => const SyncPage(
        isSnapshot: true,
        upserts: {'other': []},
        removals: {},
        cursor: '0',
        hasMore: false,
      );
}

// SembastAdapter's recovery from a dead database connection: after iOS
// Safari backgrounds a tab, WebKit can kill the IndexedDB connection under a
// live page, and every later read or write fails until a reload.
//
// These tests run the real adapter over Sembast's in-memory factory. A dead
// connection is simulated by closing the database underneath the adapter,
// which makes every operation fail with Sembast's "database is closed", one
// of the dead-connection errors the adapter recognises. The opener is
// injected so a test can count, hold, fail or poison reopens.
//
// Covers:
//   * a dead connection is reopened once and the operation retried
//   * operations failing together share ONE reopen
//   * an operation arriving mid-reopen waits for it
//   * a reopen that hangs gives up and reports the ORIGINAL failure
//   * a reopen that fails gives up for the session
//   * a second failure right after the reopen propagates and gives up
//   * any other error propagates without a reopen
//   * WebKit's messages are recognised as a dead connection

import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:reliable/reliable.dart';
import 'package:sembast/sembast_memory.dart';

const _kCollection = 'items';

void main() {
  late _Opener opener;
  late SembastAdapter adapter;
  var dbCounter = 0;

  setUp(() async {
    opener = _Opener('recovery_${dbCounter++}.db');
    adapter = SembastAdapter(
      dbName: opener.dbName,
      reopenTimeout: const Duration(milliseconds: 200),
      openDatabase: opener.open,
    );
    await adapter.init(version: 1, onUpgrade: (_, __, ___) async {});
    await adapter.writeDocument(
      collection: _kCollection,
      id: 'a',
      data: {'id': 'a'},
    );
  });

  Future<void> killConnection() => adapter.db.close();

  test('a dead connection is reopened once and the read retried', () async {
    await killConnection();
    final rows = await adapter.readCollection(collection: _kCollection);
    expect(rows, [
      {'id': 'a'},
    ]);
    expect(opener.opens, 2, reason: 'init once, reopen once');
    expect(adapter.recoveryGaveUp, isFalse);
  });

  test('a write on a dead connection lands after the reopen', () async {
    await killConnection();
    await adapter.writeDocument(
      collection: _kCollection,
      id: 'b',
      data: {'id': 'b'},
    );
    final rows = await adapter.readCollection(collection: _kCollection);
    expect(rows.map((e) => e['id']), containsAll(['a', 'b']));
    expect(opener.opens, 2);
  });

  test('operations failing together share one reopen', () async {
    await killConnection();
    await Future.wait([
      for (var i = 0; i < 3; i++)
        adapter.readCollection(collection: _kCollection),
      adapter.getQueue(),
    ]);
    expect(opener.opens, 2);
  });

  test('an operation arriving mid-reopen waits for it', () async {
    final reopenGate = Completer<void>();
    opener.reopenGate = reopenGate.future;
    await killConnection();
    final first = adapter.readCollection(collection: _kCollection);
    await pumpEventQueue();
    expect(opener.opens, 2, reason: 'the reopen has started and is held');
    var secondDone = false;
    final second = adapter
        .readCollection(collection: _kCollection)
        .whenComplete(() => secondDone = true);
    await pumpEventQueue();
    expect(secondDone, isFalse, reason: 'it must wait for the reopen');
    reopenGate.complete();
    await Future.wait([first, second]);
    expect(opener.opens, 2, reason: 'still one reopen');
  });

  test('a reopen that hangs gives up and reports the original failure',
      () async {
    opener.reopenGate = Completer<void>().future; // never completes
    await killConnection();
    await expectLater(
      adapter.readCollection(collection: _kCollection),
      throwsA(predicate((e) => '$e'.contains('database is closed'))),
    );
    expect(adapter.recoveryGaveUp, isTrue);
    // Given up: the next failure propagates at once, and the hung reopen is
    // never started a second time.
    final stopwatch = Stopwatch()..start();
    await expectLater(
      adapter.readCollection(collection: _kCollection),
      throwsA(predicate((e) => '$e'.contains('database is closed'))),
    );
    expect(stopwatch.elapsed, lessThan(const Duration(milliseconds: 150)));
    expect(opener.opens, 2);
  });

  test('a reopen that fails gives up for the session', () async {
    opener.reopenError = StateError('IndexedDB refused to open');
    await killConnection();
    await expectLater(
      adapter.readCollection(collection: _kCollection),
      throwsA(predicate((e) => '$e'.contains('database is closed'))),
    );
    expect(adapter.recoveryGaveUp, isTrue);
    await expectLater(
      adapter.readCollection(collection: _kCollection),
      throwsA(predicate((e) => '$e'.contains('database is closed'))),
    );
    expect(opener.opens, 2, reason: 'one reopen, then no more attempts');
  });

  test('a second failure right after the reopen propagates and gives up',
      () async {
    opener.reopenDead = true;
    await killConnection();
    await expectLater(
      adapter.readCollection(collection: _kCollection),
      throwsA(predicate((e) => '$e'.contains('database is closed'))),
    );
    expect(opener.opens, 2, reason: 'one reopen, never a loop');
    expect(adapter.recoveryGaveUp, isTrue);
    await expectLater(
      adapter.readCollection(collection: _kCollection),
      throwsA(predicate((e) => '$e'.contains('database is closed'))),
    );
    expect(opener.opens, 2);
  });

  test('any other error propagates without a reopen', () async {
    await expectLater(
      adapter.writeDocument(
        collection: _kCollection,
        id: 'bad',
        data: {'unsupported': Object()},
      ),
      throwsA(anything),
    );
    expect(opener.opens, 1);
    expect(adapter.recoveryGaveUp, isFalse);
  });

  test('WebKit and Sembast dead-connection messages are recognised', () {
    expect(
      SembastAdapter.isDeadConnectionError(
        Exception(
          'UnknownError: Attempt to get an index record from database '
          'without an in-progress transaction',
        ),
      ),
      isTrue,
    );
    expect(
      SembastAdapter.isDeadConnectionError(
        Exception('InvalidStateError: The database connection is closing.'),
      ),
      isTrue,
    );
    expect(
      SembastAdapter.isDeadConnectionError(Exception('database is closed')),
      isTrue,
    );
    expect(
      SembastAdapter.isDeadConnectionError(StateError('a real bug')),
      isFalse,
    );
  });
}

/// Opens in-memory Sembast databases and counts the opens. Every open after
/// the first is a reopen, which a test can hold ([reopenGate]), fail
/// ([reopenError]) or hand back already dead ([reopenDead]).
class _Opener {
  _Opener(this.dbName);

  final String dbName;
  var opens = 0;
  Future<void>? reopenGate;
  Object? reopenError;
  var reopenDead = false;

  Future<Database> open({
    required String dbName,
    required int version,
    SembastCodec? codec,
    required MigrationCallback<Database> onVersionChanged,
  }) async {
    opens++;
    if (opens > 1) {
      await reopenGate;
      final error = reopenError;
      if (error != null) throw error;
    }
    final db = await databaseFactoryMemory.openDatabase(
      dbName,
      version: version,
      codec: codec,
      onVersionChanged: onVersionChanged,
    );
    if (opens > 1 && reopenDead) await db.close();
    return db;
  }
}

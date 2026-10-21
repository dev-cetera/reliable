// ReliableRepository's guards around the cache a read writes into:
//
//   * a cache reset (purgeCollections, clearUserData, a user switch) while
//     a read is in flight stops that read's answer landing in the cache,
//     even when the reset arrives part-way through a multi-step write;
//   * a read the cache answered after a failed request logs a warning
//     (Log.alert), not an error, unless the answer was malformed or the
//     server answered with an error status;
//   * a good read whose cache copy cannot be saved is still a good read,
//     with the failed save logged as a warning on its own;
//   * the list parser knows `{items: [...]}` and refuses an unknown shape
//     instead of reading it as an empty list.
//
// In-memory fakes only: no Sembast, no HTTP.

import 'dart:async';

import 'package:df_log/df_log.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:reliable/reliable.dart';

const _kItems = 'items';
const _kOther = 'other';

void main() {
  late _Storage storage;
  late _Network network;
  late ReliableRepository repo;
  late List<String> audit;
  late List<LogItem> logs;
  late void Function(LogItem) logCallback;

  setUp(() {
    storage = _Storage();
    network = _Network();
    audit = [];
    repo = ReliableRepository(
      api: network,
      storage: storage,
      auditLog: (event, _) => audit.add(event),
    );
    logs = [];
    logCallback = Log.addCallback(logs.add);
  });

  tearDown(() => Log.removeCallback(logCallback));

  List<LogItem> logsTagged(Symbol tag, String containing) => [
        for (final item in logs)
          if (item.tags.contains(tag) && '${item.message}'.contains(containing))
            item,
      ];

  group('a cache reset while a read is in flight', () {
    test('purgeCollections stops a fetch answer landing in the cache',
        () async {
      final answer = network.hold([
        {'id': 'old-account-row'},
      ]);
      final read = repo.fetch(
        _kItems,
        '/items',
        strategy: CacheStrategy.NETWORK_OR_ELSE_CACHE,
      );
      await pumpEventQueue();
      await repo.purgeCollections([_kItems]);
      answer.release();

      final out = await read;
      expect(
        out.single['id'],
        'old-account-row',
        reason: 'the caller still gets the answer it asked for',
      );
      expect(storage.docs[_kItems] ?? {}, isEmpty);
      final cached = await repo.fetch(
        _kItems,
        '',
        strategy: CacheStrategy.CACHE_ONLY,
      );
      expect(cached, isEmpty, reason: 'nor in the memory cache');
      expect(audit, contains('REPO_CACHE_WRITE_SKIPPED_AFTER_RESET'));
    });

    test('clearUserData stops a fetch answer landing in the cache', () async {
      repo.setActiveUser('u1');
      final answer = network.hold([
        {'id': 'old-account-row'},
      ]);
      final read = repo.fetch(
        _kItems,
        '/items',
        strategy: CacheStrategy.NETWORK_OR_ELSE_CACHE,
      );
      await pumpEventQueue();
      await repo.clearUserData('u1');
      answer.release();
      await read;
      expect(storage.docs[_kItems] ?? {}, isEmpty);
    });

    test('switching active user stops a fetchOne answer landing', () async {
      repo.setActiveUser('u1');
      final answer = network.hold({'id': 'a', 'name': 'u1 data'});
      final read = repo.fetchOne(
        _kItems,
        '/items/a',
        id: 'a',
        strategy: CacheStrategy.NETWORK_OR_ELSE_CACHE,
      );
      await pumpEventQueue();
      repo.setActiveUser('u2');
      answer.release();
      expect((await read)!['name'], 'u1 data');
      expect(storage.docs[_kItems] ?? {}, isEmpty);
      final cached = await repo.fetchOne(
        _kItems,
        '',
        id: 'a',
        strategy: CacheStrategy.CACHE_ONLY,
      );
      expect(cached, isNull);
    });

    test('setting the first user does not count as a switch', () async {
      final answer = network.hold([
        {'id': 'a'},
      ]);
      final read = repo.fetch(
        _kItems,
        '/items',
        strategy: CacheStrategy.NETWORK_ONLY,
      );
      await pumpEventQueue();
      repo.setActiveUser('u1');
      answer.release();
      await read;
      expect(storage.docs[_kItems]!.keys, ['a']);
    });

    test('purging another collection leaves the read alone', () async {
      final answer = network.hold([
        {'id': 'a'},
      ]);
      final read = repo.fetch(
        _kItems,
        '/items',
        strategy: CacheStrategy.NETWORK_ONLY,
      );
      await pumpEventQueue();
      await repo.purgeCollections([_kOther]);
      answer.release();
      await read;
      expect(storage.docs[_kItems]!.keys, ['a']);
    });

    test('a purge arriving mid-write stops the write and clears what landed',
        () async {
      network.next = [
        {'id': 'a'},
        {'id': 'b'},
        {'id': 'c'},
      ];
      // Hold the first document write, so the purge lands part-way through.
      final firstWrite = Completer<void>();
      storage.writeGate = firstWrite.future;
      final read = repo.fetch(
        _kItems,
        '/items',
        strategy: CacheStrategy.NETWORK_ONLY,
      );
      await pumpEventQueue();
      expect(storage.writesStarted, 1);
      final purge = repo.purgeCollections([_kItems]);
      await pumpEventQueue();
      storage.writeGate = null;
      firstWrite.complete();
      await Future.wait([read, purge]);

      expect(storage.writesStarted, 1, reason: 'b and c were never written');
      expect(storage.docs[_kItems] ?? {}, isEmpty, reason: 'a was cleared');
    });

    test('a cache load that straddles a purge is not installed', () async {
      storage.docs[_kItems] = {
        'stale': {'id': 'stale'},
      };
      final readGate = Completer<void>();
      storage.readGate = readGate.future;
      final read = repo.fetch(
        _kItems,
        '',
        strategy: CacheStrategy.CACHE_ONLY,
      );
      await pumpEventQueue();
      final purge = repo.purgeCollections([_kItems]);
      await pumpEventQueue();
      storage.readGate = null;
      readGate.complete();
      await purge;
      expect(await read, isEmpty);
      final again = await repo.fetch(
        _kItems,
        '',
        strategy: CacheStrategy.CACHE_ONLY,
      );
      expect(again, isEmpty);
    });

    test('applyServerChange in flight across a purge is dropped', () async {
      final readGate = Completer<void>();
      storage.readGate = readGate.future;
      final change = repo.applyServerChange(
        collection: _kItems,
        id: 'a',
        data: {'name': 'old'},
      );
      await pumpEventQueue();
      final purge = repo.purgeCollections([_kItems]);
      storage.readGate = null;
      readGate.complete();
      await Future.wait([change, purge]);
      expect(storage.docs[_kItems] ?? {}, isEmpty);
    });
  });

  group('log level of a read the cache answered', () {
    test('fetch: a failed request served from cache is an alert', () async {
      storage.docs[_kItems] = {
        'a': {'id': 'a'},
      };
      network.error = const ReliableNetworkException(message: 'offline');
      final out = await repo.fetch(
        _kItems,
        '/items',
        strategy: CacheStrategy.NETWORK_OR_ELSE_CACHE,
      );
      expect(out.single['id'], 'a');
      expect(logsTagged(#alert, 'fetch items /items threw'), hasLength(1));
      expect(logsTagged(#err, 'fetch items'), isEmpty);
    });

    test('fetch: a server error status served from cache stays an error',
        () async {
      storage.docs[_kItems] = {
        'a': {'id': 'a'},
      };
      network.error = const ReliableNetworkException(
        message: 'server error',
        statusCode: 500,
      );
      final out = await repo.fetch(
        _kItems,
        '/items',
        strategy: CacheStrategy.NETWORK_OR_ELSE_CACHE,
      );
      expect(out.single['id'], 'a');
      expect(logsTagged(#err, 'fetch items /items threw'), hasLength(1));
      expect(logsTagged(#alert, 'fetch items /items threw'), isEmpty);
    });

    test('fetch: a failed request with nothing to fall back to is an error',
        () async {
      network.error = const ReliableNetworkException(message: 'offline');
      await expectLater(
        repo.fetch(_kItems, '/items', strategy: CacheStrategy.NETWORK_ONLY),
        throwsA(isA<ReliableNetworkException>()),
      );
      expect(logsTagged(#err, 'fetch items /items threw'), hasLength(1));
    });

    test('fetchOne: served from cache is an alert, a miss is an error',
        () async {
      storage.docs[_kItems] = {
        'a': {'id': 'a'},
      };
      network.error = const ReliableNetworkException(message: 'offline');
      final hit = await repo.fetchOne(
        _kItems,
        '/items/a',
        id: 'a',
        strategy: CacheStrategy.NETWORK_OR_ELSE_CACHE,
      );
      expect(hit!['id'], 'a');
      expect(logsTagged(#alert, 'fetchOne items/a'), hasLength(1));
      expect(logsTagged(#err, 'fetchOne items/a'), isEmpty);

      final miss = await repo.fetchOne(
        _kItems,
        '/items/b',
        id: 'b',
        strategy: CacheStrategy.NETWORK_OR_ELSE_CACHE,
      );
      expect(miss, isNull);
      expect(logsTagged(#err, 'fetchOne items/b'), hasLength(1));
    });

    test('fetch: a malformed answer is an error even when cache answers',
        () async {
      storage.docs[_kItems] = {
        'a': {'id': 'a'},
      };
      network.next = {'unknown': <Object>[]};
      final out = await repo.fetch(
        _kItems,
        '/items',
        strategy: CacheStrategy.NETWORK_OR_ELSE_CACHE,
      );
      expect(out.single['id'], 'a');
      expect(logsTagged(#err, 'FormatException'), hasLength(1));
    });
  });

  group('a good read whose cache copy cannot be saved', () {
    test('fetchOne keeps the answer and warns about the copy', () async {
      storage.writeError = Exception(
        'UnknownError: Attempt to get an index record from database without '
        'an in-progress transaction',
      );
      network.next = {'id': 'a', 'name': 'fresh'};
      final out = await repo.fetchOne(
        _kItems,
        '/items/a',
        id: 'a',
        strategy: CacheStrategy.NETWORK_OR_ELSE_CACHE,
      );
      expect(out!['name'], 'fresh');
      expect(logsTagged(#alert, 'cache copy was not saved'), hasLength(1));
      expect(logs.where((item) => item.tags.contains(#err)), isEmpty);
      expect(audit, contains('REPO_CACHE_WRITE_FAILED'));
      expect(audit, isNot(contains('REPO_FETCH_ONE_THREW')));
    });

    test('fetch keeps the answer and warns about the copy', () async {
      storage.writeError = Exception('disk full');
      network.next = [
        {'id': 'a'},
      ];
      final out = await repo.fetch(
        _kItems,
        '/items',
        strategy: CacheStrategy.NETWORK_ONLY,
      );
      expect(out.single['id'], 'a');
      expect(logsTagged(#alert, 'cache copy was not saved'), hasLength(1));
      expect(logs.where((item) => item.tags.contains(#err)), isEmpty);
      expect(audit, isNot(contains('REPO_FETCH_THREW')));
    });
  });

  group('list response shapes', () {
    Future<List<Map<String, dynamic>>> fetchNetworkOnly() {
      return repo.fetch(
        _kItems,
        '/items',
        strategy: CacheStrategy.NETWORK_ONLY,
      );
    }

    test('a bare array', () async {
      network.next = [
        {'id': 'a'},
      ];
      expect((await fetchNetworkOnly()).single['id'], 'a');
    });

    test('{data: [...]}', () async {
      network.next = {
        'data': [
          {'id': 'a'},
        ],
      };
      expect((await fetchNetworkOnly()).single['id'], 'a');
    });

    test('{data: {...}} is one document', () async {
      network.next = {
        'data': {'id': 'a'},
      };
      expect((await fetchNetworkOnly()).single['id'], 'a');
    });

    test('{items: [...]}', () async {
      network.next = {
        'items': [
          {'id': 'a'},
          {'id': 'b'},
        ],
        'nextCursor': 'x',
      };
      final out = await fetchNetworkOnly();
      expect(out.map((e) => e['id']), ['a', 'b']);
    });

    test('no body reads as an empty list', () async {
      network.next = null;
      expect(await fetchNetworkOnly(), isEmpty);
    });

    test('an object of unknown shape throws, naming its keys', () async {
      network.next = {
        'groups': [
          {'id': 'a'},
        ],
      };
      await expectLater(
        fetchNetworkOnly(),
        throwsA(
          isA<FormatException>().having(
            (e) => e.message,
            'message',
            contains('[groups]'),
          ),
        ),
      );
    });

    test('an unknown shape never serves the cache as if it were empty',
        () async {
      storage.docs[_kItems] = {
        'a': {'id': 'a'},
      };
      network.next = {'groups': <Object>[]};
      await expectLater(fetchNetworkOnly(), throwsA(isA<FormatException>()));
      expect(storage.docs[_kItems]!.keys, ['a'], reason: 'cache untouched');
    });

    test('an array entry that is not an object throws', () async {
      network.next = [
        {'id': 'a'},
        'b',
      ];
      await expectLater(
        fetchNetworkOnly(),
        throwsA(
          isA<FormatException>().having(
            (e) => e.message,
            'message',
            contains('entry 1'),
          ),
        ),
      );
    });
  });
}

/// Answers every request with [next], or throws [error]. [hold] makes the
/// next request wait until released.
class _Network implements NetworkAdapter {
  dynamic next;
  Object? error;
  Future<void>? _gate;

  _Held hold(dynamic answer) {
    next = answer;
    final gate = Completer<void>();
    _gate = gate.future;
    return _Held(gate);
  }

  @override
  Future<dynamic> request(
    String endpoint,
    RequestMethod method, {
    dynamic data,
    Map<String, dynamic>? queryParams,
    Map<String, String>? headers,
  }) async {
    final gate = _gate;
    _gate = null;
    await gate;
    final error = this.error;
    if (error != null) throw error;
    return next;
  }
}

class _Held {
  _Held(this._gate);
  final Completer<void> _gate;
  void release() => _gate.complete();
}

/// An in-memory store whose reads and document writes can be held
/// ([readGate], [writeGate]) or failed ([writeError]).
class _Storage implements StorageAdapter<dynamic> {
  final Map<String, Map<String, Map<String, dynamic>>> docs = {};
  final List<OfflineAction> _queue = [];
  Future<void>? readGate;
  Future<void>? writeGate;
  Object? writeError;
  var writesStarted = 0;

  @override
  Future<void> init({
    ReliableEncryption? encryption,
    required int version,
    required MigrationCallback<dynamic> onUpgrade,
  }) async {}

  @override
  Future<List<Map<String, dynamic>>> readCollection({
    required String collection,
  }) async {
    // Snapshot first, as a real read would: what it returns is what was on
    // disk when it started.
    final snapshot = docs[collection]?.values.toList() ?? [];
    await readGate;
    return snapshot;
  }

  @override
  Future<void> writeDocument({
    required String collection,
    required String id,
    required Map<String, dynamic> data,
  }) async {
    writesStarted++;
    await writeGate;
    final error = writeError;
    if (error != null) throw error;
    docs.putIfAbsent(collection, () => {})[id] = data;
  }

  @override
  Future<void> deleteDocument({
    required String collection,
    required String id,
  }) async {
    docs[collection]?.remove(id);
  }

  @override
  Future<void> clearCollection({required String collection}) async {
    docs.remove(collection);
  }

  @override
  Future<void> queueAction(OfflineAction action) async => _queue.add(action);

  @override
  Future<void> updateQueueItem(OfflineAction action) async {}

  @override
  Future<List<OfflineAction>> getQueue() async => List.of(_queue);

  @override
  Future<void> removeFromQueue(String uuid) async {
    _queue.removeWhere((a) => a.uuid == uuid);
  }

  @override
  Future<void> removeExpiredActions() async {}

  @override
  Future<void> removeActionsForUser(String userId) async {
    _queue.removeWhere((a) => a.userId == userId);
  }
}

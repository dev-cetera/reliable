// Cache-first reads (`readCacheFirst`), through a real
// ReliableRepository over an in-memory Sembast store.
//
// The timing check: on lie-fi (connected, nothing answering) a network-first
// read waits out the whole request timeout before showing rows the device
// already had; `readListCacheFirst` shows them at once and applies the
// network answer when it lands.

import 'dart:async';

import 'package:flutter_test/flutter_test.dart';
import 'package:reliable/reliable.dart';
import 'package:sembast/sembast_memory.dart';

/// The request timeout of the fake link; a real app's is seconds.
const _kTimeout = Duration(milliseconds: 600);

/// A network that answers with [rows] when [isLieFi] is false and hangs
/// until [_kTimeout] when it is true, as `ReliableNetworkAdapter` does on a
/// link that delivers nothing.
class _FakeLink implements NetworkAdapter {
  var isLieFi = false;
  List<Map<String, dynamic>> rows = [];

  /// Completed by the test to let a slow (but working) answer through.
  Completer<void>? holdAnswer;

  @override
  Future<dynamic> request(
    String endpoint,
    RequestMethod method, {
    dynamic data,
    Map<String, dynamic>? queryParams,
    Map<String, String>? headers,
  }) async {
    if (isLieFi) {
      await Future<void>.delayed(_kTimeout);
      throw const ReliableNetworkException(message: 'timed out');
    }
    await holdAnswer?.future;
    return {'items': rows};
  }
}

Future<ReliableRepository> _openRepo(_FakeLink link) async {
  final storage = SembastAdapter(
    dbName: 'cache_first_${DateTime.now().microsecondsSinceEpoch}.db',
    openDatabase: ({
      required dbName,
      required version,
      codec,
      required onVersionChanged,
    }) =>
        newDatabaseFactoryMemory()
            .openDatabase(dbName, version: version, codec: codec),
  );
  await storage.init(
    version: 1,
    onUpgrade: (db, oldVersion, newVersion) async {},
  );
  return ReliableRepository(api: link, storage: storage);
}

void main() {
  late _FakeLink link;
  late ReliableRepository repo;

  setUp(() async {
    link = _FakeLink();
    repo = await _openRepo(link);
    // A previous session cached two rows.
    link.rows = [
      {'id': 'a', 'name': 'Buy milk'},
      {'id': 'b', 'name': 'Walk dog'},
    ];
    await repo.fetch(
      'items',
      '/v1/items',
      strategy: CacheStrategy.NETWORK_ONLY,
    );
  });

  Future<List<Map<String, dynamic>>> read(CacheStrategy strategy) =>
      repo.fetch('items', '/v1/items', strategy: strategy);

  test('on lie-fi a network-first read waits out the timeout (the old wait)',
      () async {
    link.isLieFi = true;
    final watch = Stopwatch()..start();
    final rows = await read(CacheStrategy.NETWORK_OR_ELSE_CACHE);
    expect(rows, hasLength(2));
    expect(watch.elapsed, greaterThanOrEqualTo(_kTimeout));
  });

  test('on lie-fi a cache-first read shows the cached rows at once', () async {
    link.isLieFi = true;
    final shown = <int>[];
    final watch = Stopwatch()..start();
    final answer = await readListCacheFirst(
      what: 'items',
      read: read,
      apply: (rows) => shown.add(rows.length),
    );
    expect(watch.elapsed, lessThan(_kTimeout ~/ 3));
    expect(answer.source, CacheFirstSource.CACHE);
    expect(shown, [2]);
    // The background refresh gives up quietly; the cached rows stay shown.
    await answer.refreshed;
    expect(shown, [2]);
  });

  test('the network answer replaces the cached one when it lands', () async {
    link.holdAnswer = Completer<void>();
    link.rows = [
      {'id': 'a', 'name': 'Buy oat milk'},
    ];
    final shown = <List<String>>[];
    final answer = await readListCacheFirst(
      what: 'items',
      read: read,
      apply: (rows) =>
          shown.add([for (final row in rows) row['name'] as String]..sort()),
    );
    expect(shown, [
      ['Buy milk', 'Walk dog'],
    ]);
    link.holdAnswer!.complete();
    await answer.refreshed;
    expect(shown.last, ['Buy oat milk']);
  });

  test('with nothing cached it reads network-first, once', () async {
    await repo.purgeCollections(['items']);
    final shown = <int>[];
    final answer = await readListCacheFirst(
      what: 'items',
      read: read,
      apply: (rows) => shown.add(rows.length),
    );
    expect(answer.source, CacheFirstSource.NETWORK);
    expect(shown, [2]);
  });

  test('a one-document read is usable whenever a document is cached', () async {
    link.rows = [];
    await repo.fetchOne(
      'me_doc',
      '/v1/me/x',
      id: 'me',
      strategy: CacheStrategy.CACHE_ONLY,
    );
    final answer = await readOneCacheFirst(
      what: 'nothing cached',
      read: (strategy) =>
          repo.fetchOne('me_doc', '/v1/me/x', id: 'me', strategy: strategy),
      apply: (_) {},
    );
    expect(answer.source, CacheFirstSource.NETWORK);
  });
}

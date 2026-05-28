import 'package:flutter_test/flutter_test.dart';
import 'package:reliable/reliable.dart';

/// In-memory fakes — these tests don't touch Sembast or the real network.
/// They exercise the cache-strategy branches in [ReliableRepository.fetch] /
/// [ReliableRepository.fetchOne].
void main() {
  group('ReliableRepository.fetch — cache strategy branches', () {
    late _FakeStorage storage;
    late _FakeNetwork network;
    late ReliableRepository repo;

    setUp(() {
      storage = _FakeStorage();
      network = _FakeNetwork();
      repo = ReliableRepository(api: network, storage: storage);
    });

    test(
      'networkOrElseCache returns network result and updates cache',
      () async {
        network.next = [
          {'id': 'a', 'name': 'A'},
          {'id': 'b', 'name': 'B'},
        ];

        final out = await repo.fetch(
          'items',
          '/items',
          strategy: CacheStrategy.NETWORK_OR_ELSE_CACHE,
        );

        expect(out.map((e) => e['id']).toList(), ['a', 'b']);
        // Cache should now hold the same rows.
        final cached = await storage.readCollection(collection: 'items');
        expect(cached.map((e) => e['id']).toSet(), {'a', 'b'});
      },
    );

    test('networkOrElseCache returns cache when network fails', () async {
      // Pre-seed the cache.
      await storage.writeDocument(
        collection: 'items',
        id: 'a',
        data: {'id': 'a', 'name': 'cached'},
      );
      network.shouldThrow = true;

      final out = await repo.fetch(
        'items',
        '/items',
        strategy: CacheStrategy.NETWORK_OR_ELSE_CACHE,
      );

      expect(out, hasLength(1));
      expect(out.single['name'], 'cached');
    });

    test(
      'networkOrElseCache returns empty when network fails and cache empty',
      () async {
        network.shouldThrow = true;

        final out = await repo.fetch(
          'items',
          '/items',
          strategy: CacheStrategy.NETWORK_OR_ELSE_CACHE,
        );

        expect(out, isEmpty);
      },
    );

    test(
      'cacheOrElseNetwork rethrows when cache empty + network throws',
      () async {
        network.shouldThrow = true;

        expect(
          () => repo.fetch(
            'items',
            '/items',
            strategy: CacheStrategy.CACHE_OR_ELSE_NETWORK,
          ),
          throwsA(isA<ReliableNetworkException>()),
        );
      },
    );

    test('cacheOnly never hits the network', () async {
      await storage.writeDocument(
        collection: 'items',
        id: 'a',
        data: {'id': 'a', 'name': 'cached'},
      );
      network.shouldThrow = true; // Would blow up if invoked.

      final out = await repo.fetch(
        'items',
        '/items',
        strategy: CacheStrategy.CACHE_ONLY,
      );

      expect(out.single['id'], 'a');
      expect(network.callCount, 0);
    });

    test('networkOnly always hits the network and refreshes cache', () async {
      await storage.writeDocument(
        collection: 'items',
        id: 'stale',
        data: {'id': 'stale'},
      );
      network.next = [
        {'id': 'fresh'},
      ];

      final out = await repo.fetch(
        'items',
        '/items',
        strategy: CacheStrategy.NETWORK_ONLY,
      );

      expect(out.single['id'], 'fresh');
      expect(network.callCount, 1);
    });
  });

  group('ReliableRepository.fetchOne', () {
    late _FakeStorage storage;
    late _FakeNetwork network;
    late ReliableRepository repo;

    setUp(() {
      storage = _FakeStorage();
      network = _FakeNetwork();
      repo = ReliableRepository(api: network, storage: storage);
    });

    test(
      'cacheOrElseNetwork returns cache hit without hitting network',
      () async {
        await storage.writeDocument(
          collection: 'items',
          id: 'a',
          data: {'id': 'a', 'name': 'cached'},
        );

        final out = await repo.fetchOne('items', '/items', id: 'a');

        expect(out!['name'], 'cached');
        expect(network.callCount, 0);
      },
    );

    test('falls back to network when cache miss', () async {
      network.next = {'id': 'a', 'name': 'fetched'};

      final out = await repo.fetchOne('items', '/items', id: 'a');

      expect(out!['name'], 'fetched');
      expect(network.callCount, 1);
    });
  });
}

class _FakeNetwork implements NetworkAdapter {
  bool shouldThrow = false;
  dynamic next;
  int callCount = 0;

  @override
  Future<dynamic> request(
    String endpoint,
    RequestMethod method, {
    dynamic data,
    Map<String, dynamic>? queryParams,
    Map<String, String>? headers,
  }) async {
    callCount += 1;
    if (shouldThrow) {
      throw const ReliableNetworkException(message: 'boom');
    }
    return next;
  }
}

class _FakeStorage implements StorageAdapter<dynamic> {
  final Map<String, Map<String, Map<String, dynamic>>> _docs = {};
  final List<OfflineAction> _queue = [];

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
    return _docs[collection]?.values.toList() ?? [];
  }

  @override
  Future<void> writeDocument({
    required String collection,
    required String id,
    required Map<String, dynamic> data,
  }) async {
    _docs.putIfAbsent(collection, () => {})[id] = data;
  }

  @override
  Future<void> deleteDocument({
    required String collection,
    required String id,
  }) async {
    _docs[collection]?.remove(id);
  }

  @override
  Future<void> clearCollection({required String collection}) async {
    _docs[collection]?.clear();
  }

  @override
  Future<void> queueAction(OfflineAction action) async {
    _queue.add(action);
  }

  @override
  Future<void> updateQueueItem(OfflineAction action) async {
    final i = _queue.indexWhere((a) => a.uuid == action.uuid);
    if (i >= 0) _queue[i] = action;
  }

  @override
  Future<List<OfflineAction>> getQueue() async => List.of(_queue);

  @override
  Future<void> removeFromQueue(String uuid) async {
    _queue.removeWhere((a) => a.uuid == uuid);
  }

  @override
  Future<void> removeExpiredActions() async {
    final now = DateTime.now().millisecondsSinceEpoch;
    _queue.removeWhere((a) => a.expiresAt != null && a.expiresAt! < now);
  }

  @override
  Future<void> removeActionsForUser(String userId) async {
    _queue.removeWhere((a) => a.userId == userId);
  }
}

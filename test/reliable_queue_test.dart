import 'package:flutter_test/flutter_test.dart';
import 'package:reliable/reliable.dart';

/// Exercises the write/queue/replay path of [ReliableRepository] against
/// in-memory fakes so the test never touches Sembast or HTTP.
void main() {
  group('ReliableRepository.write — optimistic update + queueing', () {
    late _FakeStorage storage;
    late _FakeNetwork network;
    late ReliableRepository repo;

    setUp(() {
      storage = _FakeStorage();
      network = _FakeNetwork();
      repo = ReliableRepository(api: network, storage: storage);
      repo.setActiveUser('u1');
    });

    test('write succeeds → action drains from the queue', () async {
      await repo.write(
        collection: 'items',
        endpoint: '/items',
        method: RequestMethod.POST,
        data: {'description': 'X'},
        id: 'id-1',
      );

      // The unawaited _processQueue from `write` may still be running. Wait
      // for it to settle before inspecting state.
      await _settle();

      expect(await repo.getPendingCount(), 0);
      // Network adapter received the POST.
      expect(network.callCount, 1);
    });

    test(
      'write fails (transient) → action stays in queue with retryCount > 0',
      () async {
        network.shouldThrow = true;
        await repo.write(
          collection: 'items',
          endpoint: '/items',
          method: RequestMethod.POST,
          data: {'description': 'Y'},
          id: 'id-2',
        );
        await _settle();

        final pending = await repo.getPendingActions();
        expect(pending, hasLength(1));
        expect(pending.single.retryCount, greaterThan(0));
      },
    );

    test('write tags the action with the active user', () async {
      network.shouldThrow = true;
      await repo.write(
        collection: 'items',
        endpoint: '/items',
        method: RequestMethod.POST,
        data: {'description': 'tagged'},
      );
      await _settle();
      final pending = await repo.getPendingActions();
      expect(pending.single.userId, 'u1');
    });

    test('clearUserData removes the active user\'s pending actions', () async {
      network.shouldThrow = true;
      await repo.write(
        collection: 'items',
        endpoint: '/items',
        method: RequestMethod.POST,
        data: {'a': 1},
      );
      await _settle();
      expect(await repo.getPendingCount(), 1);

      await repo.clearUserData('u1');
      expect(await repo.getPendingCount(), 0);
    });

    test('fatal errors drop the queued action immediately', () async {
      network.thrownError = const ReliableNetworkException(
        message: 'bad request',
        statusCode: 400,
        isFatal: true,
      );
      await repo.write(
        collection: 'items',
        endpoint: '/items',
        method: RequestMethod.POST,
        data: {'a': 1},
        id: 'id-fatal',
      );
      await _settle();
      expect(await repo.getPendingCount(), 0);
    });
  });

  group('ReliableRepository — server-push integration', () {
    late _FakeStorage storage;
    late _FakeNetwork network;
    late ReliableRepository repo;

    setUp(() {
      storage = _FakeStorage();
      network = _FakeNetwork();
      repo = ReliableRepository(api: network, storage: storage);
    });

    test('applyServerChange writes to cache without queueing a sync', () async {
      await repo.applyServerChange(
        collection: 'items',
        id: 'a',
        data: {'id': 'a', 'name': 'pushed'},
      );
      expect(await repo.getPendingCount(), 0);
      // cacheOnly proves it landed in cache.
      final out = await repo.fetch(
        'items',
        '/items',
        strategy: CacheStrategy.CACHE_ONLY,
      );
      expect(out.single['name'], 'pushed');
    });

    test('applyServerDelete removes the cached document', () async {
      await repo.applyServerChange(
        collection: 'items',
        id: 'a',
        data: {'id': 'a'},
      );
      await repo.applyServerDelete(collection: 'items', id: 'a');
      final out = await repo.fetch(
        'items',
        '/items',
        strategy: CacheStrategy.CACHE_ONLY,
      );
      expect(out, isEmpty);
    });
  });
}

/// Pumps the microtask queue a few times so any unawaited futures spawned by
/// [ReliableRepository.write] settle before the test inspects state.
Future<void> _settle() async {
  for (var i = 0; i < 5; i++) {
    await Future<void>.delayed(Duration.zero);
  }
}

class _FakeNetwork implements NetworkAdapter {
  bool shouldThrow = false;
  ReliableNetworkException? thrownError;
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
    if (thrownError != null) throw thrownError!;
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

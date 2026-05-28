import 'package:flutter_test/flutter_test.dart';
import 'package:reliable/reliable.dart';

/// In-memory fakes — see `reliable_repository_test.dart` for the canonical
/// shape. Duplicated here to keep this test file self-contained.
void main() {
  group('DataMigration — cache schema migration', () {
    test('upgrades stale on-disk docs to the latest version on first read',
        () async {
      final storage = _FakeStorage();
      // Seed disk with a doc tagged at v1 plus an untagged legacy doc (v0).
      await storage.writeDocument(
        collection: 'users',
        id: 'alice',
        data: {
          'id': 'alice',
          'firstName': 'Alice',
          ReliableSchema.colSchemaVersion: 1,
        },
      );
      await storage.writeDocument(
        collection: 'users',
        id: 'bob',
        data: {'id': 'bob', 'firstName': 'Bob'}, // legacy, no version tag
      );

      final repo = ReliableRepository(
        api: _NoNetwork(),
        storage: storage,
        dataMigrations: {
          'users': [
            DataMigration(
              toVersion: 1,
              migrate: (doc) => {
                ...doc,
                'firstName': doc['firstName'] ?? doc['name'],
              }..remove('name'),
            ),
            DataMigration(
              toVersion: 2,
              migrate: (doc) => {
                ...doc,
                'name': doc['firstName'],
              }..remove('firstName'),
            ),
            DataMigration(
              toVersion: 3,
              migrate: (doc) => {...doc, 'verified': false},
            ),
          ],
        },
      );

      final out = await repo.fetch(
        'users',
        '/users',
        strategy: CacheStrategy.CACHE_ONLY,
      );

      final alice = out.firstWhere((e) => e['id'] == 'alice');
      final bob = out.firstWhere((e) => e['id'] == 'bob');

      expect(alice['name'], 'Alice');
      expect(alice['verified'], false);
      expect(alice.containsKey('firstName'), false);
      expect(
        alice.containsKey(ReliableSchema.colSchemaVersion),
        false,
        reason: 'schema version key must be stripped before returning',
      );

      expect(bob['name'], 'Bob');
      expect(bob['verified'], false);
      expect(bob.containsKey('firstName'), false);

      // On-disk docs are re-persisted with the latest version tag.
      final disk = await storage.readCollection(collection: 'users');
      for (final doc in disk) {
        expect(
          doc[ReliableSchema.colSchemaVersion],
          3,
          reason: 'persisted docs must carry the latest version tag',
        );
      }
    });

    test('drops docs whose migration throws and emits an audit event',
        () async {
      final storage = _FakeStorage();
      await storage.writeDocument(
        collection: 'users',
        id: 'bad',
        data: {'id': 'bad', 'corruptField': true},
      );
      await storage.writeDocument(
        collection: 'users',
        id: 'good',
        data: {'id': 'good', 'name': 'Good'},
      );

      final auditEvents = <String>[];
      final repo = ReliableRepository(
        api: _NoNetwork(),
        storage: storage,
        auditLog: (event, _) => auditEvents.add(event),
        dataMigrations: {
          'users': [
            DataMigration(
              toVersion: 1,
              migrate: (doc) {
                if (doc['corruptField'] == true) {
                  throw const FormatException('cannot migrate');
                }
                return doc;
              },
            ),
          ],
        },
      );

      final out = await repo.fetch(
        'users',
        '/users',
        strategy: CacheStrategy.CACHE_ONLY,
      );

      expect(out.length, 1);
      expect(out.first['id'], 'good');
      expect(auditEvents, contains('REPO_MIGRATION_FAILED'));
    });

    test('tags freshly-fetched docs with the latest version on write',
        () async {
      final storage = _FakeStorage();
      final network = _ListNetwork()
        ..next = [
          {'id': 'x', 'name': 'X'},
        ];

      final repo = ReliableRepository(
        api: network,
        storage: storage,
        dataMigrations: {
          'items': [DataMigration(toVersion: 2, migrate: (doc) => doc)],
        },
      );

      await repo.fetch(
        'items',
        '/items',
        strategy: CacheStrategy.NETWORK_ONLY,
      );

      final disk = await storage.readCollection(collection: 'items');
      expect(disk.single[ReliableSchema.colSchemaVersion], 2);
    });

    test('no migrations registered = no schema-version field on disk',
        () async {
      final storage = _FakeStorage();
      final network = _ListNetwork()
        ..next = [
          {'id': 'x', 'name': 'X'},
        ];

      final repo = ReliableRepository(api: network, storage: storage);
      await repo.fetch(
        'items',
        '/items',
        strategy: CacheStrategy.NETWORK_ONLY,
      );

      final disk = await storage.readCollection(collection: 'items');
      expect(
        disk.single.containsKey(ReliableSchema.colSchemaVersion),
        false,
        reason: 'no migrations = no schema tag on disk (zero overhead)',
      );
    });

    test('rejects duplicate toVersion in the same collection', () {
      expect(
        () => ReliableRepository(
          api: _NoNetwork(),
          storage: _FakeStorage(),
          dataMigrations: {
            'users': [
              DataMigration(toVersion: 1, migrate: (doc) => doc),
              DataMigration(toVersion: 1, migrate: (doc) => doc),
            ],
          },
        ),
        throwsArgumentError,
      );
    });

    test('rejects toVersion < 1', () {
      expect(
        () => ReliableRepository(
          api: _NoNetwork(),
          storage: _FakeStorage(),
          dataMigrations: {
            'users': [DataMigration(toVersion: 0, migrate: (doc) => doc)],
          },
        ),
        throwsArgumentError,
      );
    });
  });
}

class _NoNetwork implements NetworkAdapter {
  @override
  Future<dynamic> request(
    String endpoint,
    RequestMethod method, {
    dynamic data,
    Map<String, dynamic>? queryParams,
    Map<String, String>? headers,
  }) async =>
      throw const ReliableNetworkException(message: 'no network');
}

class _ListNetwork implements NetworkAdapter {
  List<Map<String, dynamic>> next = const [];
  @override
  Future<dynamic> request(
    String endpoint,
    RequestMethod method, {
    dynamic data,
    Map<String, dynamic>? queryParams,
    Map<String, String>? headers,
  }) async =>
      next;
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

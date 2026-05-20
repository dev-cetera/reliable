import 'dart:convert';
import 'package:sembast/sembast.dart';
import '../reliable_core.dart';

import 'sembast_adapter_stub.dart'
    if (dart.library.io) 'sembast_adapter_native.dart'
    if (dart.library.js_interop) 'sembast_adapter_web.dart'
    if (dart.library.js) 'sembast_adapter_web.dart'
    if (dart.library.html) 'sembast_adapter_web.dart'
    as platform;

class SembastAdapter implements StorageAdapter<Database> {
  Database? _db;
  final String dbName;
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

  SembastAdapter({this.dbName = 'reliable_data.db'});

  Database get db {
    if (_db == null) throw StateError('Database not initialized.');
    return _db!;
  }

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

    _db = await platform.openSembastDatabase(
      dbName: dbName,
      version: version,
      codec: codec,
      onVersionChanged: onUpgrade,
    );
  }

  // --- CRUD ---
  @override
  Future<List<Map<String, dynamic>>> readCollection({
    required String collection,
  }) async {
    final store = stringMapStoreFactory.store(collection);
    final records = await store.find(db);
    return records.map((e) => Map<String, dynamic>.from(e.value)).toList();
  }

  @override
  Future<void> writeDocument({
    required String collection,
    required String id,
    required Map<String, dynamic> data,
  }) async {
    await stringMapStoreFactory.store(collection).record(id).put(db, data);
  }

  @override
  Future<void> deleteDocument({
    required String collection,
    required String id,
  }) async {
    await stringMapStoreFactory.store(collection).record(id).delete(db);
  }

  @override
  Future<void> clearCollection({required String collection}) async {
    await stringMapStoreFactory.store(collection).delete(db);
  }

  // --- QUEUE ---
  @override
  Future<void> queueAction(OfflineAction action) async {
    await _queueStore.add(db, action.toMap());
  }

  @override
  Future<List<OfflineAction>> getQueue() async {
    final finder = Finder(sortOrders: [SortOrder(ReliableSchema.colTimestamp)]);
    final snapshots = await _queueStore.find(db, finder: finder);
    return snapshots.map((e) => OfflineAction.fromMap(e.value)).toList();
  }

  @override
  Future<void> removeFromQueue(String uuid) async {
    await _queueStore.delete(
      db,
      finder: Finder(filter: Filter.equals(ReliableSchema.colUuid, uuid)),
    );
  }

  @override
  Future<void> updateQueueItem(OfflineAction action) async {
    await _queueStore.update(
      db,
      action.toMap(),
      finder: Finder(
        filter: Filter.equals(ReliableSchema.colUuid, action.uuid),
      ),
    );
  }

  // --- SECURITY ---
  @override
  Future<void> removeExpiredActions() async {
    final now = DateTime.now().millisecondsSinceEpoch;
    final finder = Finder(
      filter: Filter.and([
        Filter.notEquals(ReliableSchema.colExpiresAt, null),
        Filter.lessThan(ReliableSchema.colExpiresAt, now),
      ]),
    );
    await _queueStore.delete(db, finder: finder);
  }

  @override
  Future<void> removeActionsForUser(String userId) async {
    await _queueStore.delete(
      db,
      finder: Finder(filter: Filter.equals(ReliableSchema.colUserId, userId)),
    );
  }
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

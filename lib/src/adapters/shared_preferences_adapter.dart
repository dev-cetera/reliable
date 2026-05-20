import 'dart:convert';
import 'package:df_log/df_log.dart';
import 'package:shared_preferences/shared_preferences.dart';
import '../reliable_core.dart';

class SharedPreferencesAdapter implements StorageAdapter<SharedPreferences> {
  SharedPreferences? _prefs;
  ReliableEncryption? _encryption;

  // Metadata Keys
  static const String _keyVersion = 'reliable_db_version';
  static const String _keyQueueIndex = 'reliable_queue_idx';

  // Key Generators
  String _colIdxKey(String col) => 'reliable_idx_$col';
  String _itemKey(String col, String id) => 'reliable_dat_${col}_$id';
  String _queueItemKey(String uuid) => 'reliable_q_$uuid';

  SharedPreferences get prefs {
    if (_prefs == null) throw StateError('SharedPreferences not initialized.');
    return _prefs!;
  }

  @override
  Future<void> init({
    ReliableEncryption? encryption,
    required int version,
    required MigrationCallback<SharedPreferences> onUpgrade,
  }) async {
    _encryption = encryption;
    _prefs = await SharedPreferences.getInstance();

    final currentVersion = _prefs!.getInt(_keyVersion) ?? 0;
    if (currentVersion < version) {
      await onUpgrade(_prefs!, currentVersion, version);
      await _prefs!.setInt(_keyVersion, version);
    }
  }

  // HELPERS (Encryption)

  String _processInput(String input) {
    if (_encryption == null) return input;
    return _encryption!.encrypt(input);
  }

  String _processOutput(String output) {
    if (_encryption == null) return output;
    return _encryption!.decrypt(output);
  }

  // CACHE OPERATIONS

  @override
  Future<List<Map<String, dynamic>>> readCollection({
    required String collection,
  }) async {
    final ids = _prefs!.getStringList(_colIdxKey(collection)) ?? [];
    final results = <Map<String, dynamic>>[];

    for (final id in ids) {
      final raw = _prefs!.getString(_itemKey(collection, id));
      if (raw != null) {
        try {
          final decrypted = _processOutput(raw);
          results.add(jsonDecode(decrypted) as Map<String, dynamic>);
        } catch (error) {
          Log.alert(
            'reliable shared_prefs corrupted row $collection/$id: $error',
          );
        }
      }
    }
    return results;
  }

  @override
  Future<void> writeDocument({
    required String collection,
    required String id,
    required Map<String, dynamic> data,
  }) async {
    final jsonStr = jsonEncode(data);
    await _prefs!.setString(_itemKey(collection, id), _processInput(jsonStr));

    final idxKey = _colIdxKey(collection);
    final ids = _prefs!.getStringList(idxKey) ?? [];
    if (!ids.contains(id)) {
      ids.add(id);
      await _prefs!.setStringList(idxKey, ids);
    }
  }

  @override
  Future<void> deleteDocument({
    required String collection,
    required String id,
  }) async {
    await _prefs!.remove(_itemKey(collection, id));

    final idxKey = _colIdxKey(collection);
    final ids = _prefs!.getStringList(idxKey) ?? [];
    if (ids.remove(id)) {
      await _prefs!.setStringList(idxKey, ids);
    }
  }

  @override
  Future<void> clearCollection({required String collection}) async {
    final idxKey = _colIdxKey(collection);
    final ids = _prefs!.getStringList(idxKey) ?? [];

    for (final id in ids) {
      await _prefs!.remove(_itemKey(collection, id));
    }
    await _prefs!.remove(idxKey);
  }

  // QUEUE OPERATIONS

  @override
  Future<void> queueAction(OfflineAction action) async {
    final jsonStr = jsonEncode(action.toMap());
    await _prefs!.setString(_queueItemKey(action.uuid), _processInput(jsonStr));

    final ids = _prefs!.getStringList(_keyQueueIndex) ?? [];
    if (!ids.contains(action.uuid)) {
      ids.add(action.uuid);
      await _prefs!.setStringList(_keyQueueIndex, ids);
    }
  }

  @override
  Future<List<OfflineAction>> getQueue() async {
    final ids = _prefs!.getStringList(_keyQueueIndex) ?? [];
    final actions = <OfflineAction>[];

    for (final uuid in ids) {
      final raw = _prefs!.getString(_queueItemKey(uuid));
      if (raw != null) {
        try {
          final decrypted = _processOutput(raw);
          final map = jsonDecode(decrypted) as Map<String, dynamic>;
          actions.add(OfflineAction.fromMap(map));
        } catch (error) {
          Log.alert(
            'reliable shared_prefs corrupted queue entry $uuid: $error',
          );
        }
      }
    }

    // Sort by timestamp (FIFO)
    actions.sort((a, b) => a.timestamp.compareTo(b.timestamp));
    return actions;
  }

  @override
  Future<void> removeFromQueue(String uuid) async {
    await _prefs!.remove(_queueItemKey(uuid));

    final ids = _prefs!.getStringList(_keyQueueIndex) ?? [];
    if (ids.remove(uuid)) {
      await _prefs!.setStringList(_keyQueueIndex, ids);
    }
  }

  @override
  Future<void> updateQueueItem(OfflineAction action) async {
    final jsonStr = jsonEncode(action.toMap());
    await _prefs!.setString(_queueItemKey(action.uuid), _processInput(jsonStr));
  }

  // SECURITY & HOUSEKEEPING

  @override
  Future<void> removeExpiredActions() async {
    final actions = await getQueue();
    final now = DateTime.now().millisecondsSinceEpoch;

    for (final action in actions) {
      if (action.expiresAt != null && action.expiresAt! < now) {
        await removeFromQueue(action.uuid);
      }
    }
  }

  @override
  Future<void> removeActionsForUser(String userId) async {
    final actions = await getQueue();
    for (final action in actions) {
      if (action.userId == userId) {
        await removeFromQueue(action.uuid);
      }
    }
  }
}

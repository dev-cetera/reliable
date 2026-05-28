// Regression test for the Sembast store-name collision that crashed every
// `fetch` with `networkOrElseCache` after the app queued an offline write.
//
// What was happening:
//   - The host app maintained an `offline_queue` collection of pending
//     mutations, written via `repo.applyServerChange` → Sembast's
//     `stringMapStoreFactory.store('offline_queue').record('pending:<uuid>').put(...)`.
//   - `ReliableRepository`'s internal action queue used
//     `intMapStoreFactory.store('offline_queue')` — the SAME underlying store
//     in Sembast (the K type is just a Dart-side hint, the store identity is
//     the name).
//   - On the next offline boot, `repo.fetch` falls back to cache, calls
//     `_mergeOfflineChanges`, calls `_storage.getQueue()`, which calls
//     `_queueStore.find()`. Sembast tries to deserialize the record snapshots
//     with `key as int`, hits the app's string keys, and throws
//     `type 'String' is not a subtype of type 'int' in type cast`.
//   - That exception propagates up through `repo.fetch` → `items.start` →
//     `onLogin`, where it gets caught at `_handleAuthLoadError` — leaving
//     services in a partial state and the timeline empty. Disk was always
//     intact. The wipe was a UI symptom of a mid-boot crash.
//
// The fix: namespace the reliable internal queue under
// `__reliable_action_queue` so it cannot collide with any app collection.

import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

void main() {
  test(
    'intMapStoreFactory and stringMapStoreFactory with the same name share '
    'storage — proof that the `offline_queue` namespace was unsafe',
    () async {
      final db = await databaseFactoryMemory.openDatabase('a.db');
      final stringStore = stringMapStoreFactory.store('offline_queue');
      final intStore = intMapStoreFactory.store('offline_queue');

      await stringStore.record('pending:abc-123').put(db, {
        'id': 'pending:abc-123',
        'method': 'POST',
        'path': '/v1/items',
      });

      // The bug: this throws because Sembast casts the String record key to
      // int when materializing the snapshots for the int-map view.
      Object? caught;
      try {
        await intStore.find(db);
      } catch (e) {
        caught = e;
      }
      await db.close();

      expect(
        caught.toString(),
        contains("type 'String' is not a subtype of type 'int'"),
        reason: 'If this assertion fails, sembast no longer treats stores with '
            'the same name + different K type as the same store. The '
            'namespacing fix in sembast_adapter.dart can be reverted.',
      );
    },
  );

  test(
    'namespaced internal queue (`__reliable_action_queue`) does NOT collide '
    'with an app collection named `offline_queue`',
    () async {
      final db = await databaseFactoryMemory.openDatabase('b.db');
      final appStore = stringMapStoreFactory.store('offline_queue');
      final reliableQueue = intMapStoreFactory.store(
        '__reliable_action_queue',
      );

      await appStore.record('pending:abc-123').put(db, {
        'id': 'pending:abc-123',
        'method': 'POST',
        'path': '/v1/items',
      });

      // Should not throw — different store names mean different storage.
      final snapshots = await reliableQueue.find(db);
      expect(
        snapshots,
        isEmpty,
        reason:
            'The reliable internal queue must be empty; the app records must '
            'live only in the app store.',
      );
      await db.close();
    },
  );
}

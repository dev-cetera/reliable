import 'dart:convert';
import 'package:sqflite/sqflite.dart';
import 'package:path/path.dart';
import '../reliable_core.dart';

class SqliteAdapter implements StorageAdapter<Database> {
  Database? _db;
  final String dbName;
  ReliableEncryption? _encryption;

  SqliteAdapter({this.dbName = 'reliable.db'});

  Database get db {
    if (_db == null) {
      throw StateError('Database not initialized. Call init() first.');
    }
    return _db!;
  }

  @override
  Future<void> init({
    ReliableEncryption? encryption,
    required int version,
    required MigrationCallback<Database> onUpgrade,
  }) async {
    _encryption = encryption;
    final databasesPath = await getDatabasesPath();
    final path = join(databasesPath, dbName);

    _db = await openDatabase(
      path,
      version: version,
      onCreate: (db, version) async {
        await _createTables(db);
      },
      onUpgrade: onUpgrade,
    );
  }

  Future<void> _createTables(Database db) async {
    await db.execute('''
      CREATE TABLE ${ReliableSchema.tableCache} (
        ${ReliableSchema.colCollection} TEXT NOT NULL,
        ${ReliableSchema.colDocId} TEXT NOT NULL,
        ${ReliableSchema.colData} TEXT NOT NULL,
        PRIMARY KEY (${ReliableSchema.colCollection}, ${ReliableSchema.colDocId})
      )
    ''');

    await db.execute('''
      CREATE TABLE ${ReliableSchema.tableQueue} (
        ${ReliableSchema.colUuid} TEXT PRIMARY KEY,
        ${ReliableSchema.colCollection} TEXT NOT NULL,
        ${ReliableSchema.colEndpoint} TEXT NOT NULL,
        ${ReliableSchema.colMethod} TEXT NOT NULL,
        ${ReliableSchema.colDocId} TEXT NOT NULL,
        ${ReliableSchema.colPayload} TEXT,
        ${ReliableSchema.colTimestamp} INTEGER NOT NULL,
        ${ReliableSchema.colRetryCount} INTEGER NOT NULL DEFAULT 0,
        ${ReliableSchema.colNextTryAt} INTEGER NOT NULL DEFAULT 0,
        ${ReliableSchema.colUserId} TEXT,
        ${ReliableSchema.colExpiresAt} INTEGER
      )
    ''');

    await db.execute(
      'CREATE INDEX idx_queue_ts ON ${ReliableSchema.tableQueue} (${ReliableSchema.colTimestamp})',
    );
  }

  String _processInput(String input) {
    if (_encryption == null) return input;
    return _encryption!.encrypt(input);
  }

  String _processOutput(String output) {
    if (_encryption == null) return output;
    return _encryption!.decrypt(output);
  }

  @override
  Future<List<Map<String, dynamic>>> readCollection({
    required String collection,
  }) async {
    final List<Map<String, dynamic>> rows = await db.query(
      ReliableSchema.tableCache,
      columns: [ReliableSchema.colData],
      where: '${ReliableSchema.colCollection} = ?',
      whereArgs: [collection],
    );

    return rows.map((row) {
      final rawData = row[ReliableSchema.colData] as String;
      final decrypted = _processOutput(rawData);
      return jsonDecode(decrypted) as Map<String, dynamic>;
    }).toList();
  }

  @override
  Future<void> writeDocument({
    required String collection,
    required String id,
    required Map<String, dynamic> data,
  }) async {
    final jsonStr = jsonEncode(data);
    final storedData = _processInput(jsonStr);

    await db.insert(
      ReliableSchema.tableCache,
      {
        ReliableSchema.colCollection: collection,
        ReliableSchema.colDocId: id,
        ReliableSchema.colData: storedData,
      },
      conflictAlgorithm: ConflictAlgorithm.replace,
    );
  }

  @override
  Future<void> deleteDocument({
    required String collection,
    required String id,
  }) async {
    await db.delete(
      ReliableSchema.tableCache,
      where:
          '${ReliableSchema.colCollection} = ? AND ${ReliableSchema.colDocId} = ?',
      whereArgs: [collection, id],
    );
  }

  @override
  Future<void> clearCollection({required String collection}) async {
    await db.delete(
      ReliableSchema.tableCache,
      where: '${ReliableSchema.colCollection} = ?',
      whereArgs: [collection],
    );
  }

  @override
  Future<void> queueAction(OfflineAction action) async {
    final map = action.toMap();
    if (map[ReliableSchema.colPayload] != null) {
      final jsonPayload = jsonEncode(map[ReliableSchema.colPayload]);
      map[ReliableSchema.colPayload] = _processInput(jsonPayload);
    }
    await db.insert(
      ReliableSchema.tableQueue,
      map,
      conflictAlgorithm: ConflictAlgorithm.replace,
    );
  }

  @override
  Future<List<OfflineAction>> getQueue() async {
    final List<Map<String, dynamic>> rows = await db.query(
      ReliableSchema.tableQueue,
      orderBy: '${ReliableSchema.colTimestamp} ASC',
    );

    return rows.map((row) {
      final mutableRow = Map<String, dynamic>.from(row);
      if (mutableRow[ReliableSchema.colPayload] != null) {
        final rawPayload = mutableRow[ReliableSchema.colPayload] as String;
        final decrypted = _processOutput(rawPayload);
        mutableRow[ReliableSchema.colPayload] = jsonDecode(decrypted);
      }
      return OfflineAction.fromMap(mutableRow);
    }).toList();
  }

  @override
  Future<void> removeFromQueue(String uuid) async {
    await db.delete(
      ReliableSchema.tableQueue,
      where: '${ReliableSchema.colUuid} = ?',
      whereArgs: [uuid],
    );
  }

  @override
  Future<void> updateQueueItem(OfflineAction action) async {
    final map = action.toMap();
    if (map[ReliableSchema.colPayload] != null) {
      final jsonPayload = jsonEncode(map[ReliableSchema.colPayload]);
      map[ReliableSchema.colPayload] = _processInput(jsonPayload);
    }
    await db.update(
      ReliableSchema.tableQueue,
      map,
      where: '${ReliableSchema.colUuid} = ?',
      whereArgs: [action.uuid],
    );
  }

  @override
  Future<void> removeExpiredActions() async {
    final now = DateTime.now().millisecondsSinceEpoch;
    await db.delete(
      ReliableSchema.tableQueue,
      where:
          '${ReliableSchema.colExpiresAt} IS NOT NULL AND ${ReliableSchema.colExpiresAt} < ?',
      whereArgs: [now],
    );
  }

  @override
  Future<void> removeActionsForUser(String userId) async {
    await db.delete(
      ReliableSchema.tableQueue,
      where: '${ReliableSchema.colUserId} = ?',
      whereArgs: [userId],
    );
  }
}

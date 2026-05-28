import 'package:sembast_web/sembast_web.dart';

Future<Database> openSembastDatabase({
  required String dbName,
  required int version,
  SembastCodec? codec,
  required Future<void> Function(Database db, int oldVersion, int newVersion)
      onVersionChanged,
}) async {
  return databaseFactoryWeb.openDatabase(
    dbName,
    version: version,
    codec: codec,
    onVersionChanged: (Database db, int oldVersion, int newVersion) async {
      if (oldVersion < newVersion)
        await onVersionChanged(db, oldVersion, newVersion);
    },
  );
}

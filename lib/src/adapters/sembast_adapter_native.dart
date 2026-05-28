import 'package:path_provider/path_provider.dart';
import 'package:path/path.dart';
import 'package:sembast/sembast_io.dart';

Future<Database> openSembastDatabase({
  required String dbName,
  required int version,
  SembastCodec? codec,
  required Future<void> Function(Database db, int oldVersion, int newVersion)
      onVersionChanged,
}) async {
  final dir = await getApplicationDocumentsDirectory();
  await dir.create(recursive: true);
  final path = join(dir.path, dbName);
  return databaseFactoryIo.openDatabase(
    path,
    version: version,
    codec: codec,
    onVersionChanged: (db, oldVersion, newVersion) async {
      if (oldVersion < newVersion) {
        await onVersionChanged(db, oldVersion, newVersion);
      }
    },
  );
}

import 'package:sembast/sembast.dart';

Future<Database> openSembastDatabase({
  required String dbName,
  required int version,
  SembastCodec? codec,
  required Future<void> Function(Database db, int oldVersion, int newVersion)
      onVersionChanged,
}) {
  throw UnsupportedError(
    'openSembastDatabase() is not supported on this platform.',
  );
}

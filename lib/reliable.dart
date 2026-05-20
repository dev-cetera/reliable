/// An offline-first, secure, and robust data repository for Dart & Flutter.
library;

export 'src/reliable_core.dart';
export 'src/reliable_repository.dart';
export 'src/adapters/sembast_adapter.dart';
export 'src/adapters/shared_preferences_adapter.dart';
export 'src/encryption/reliable_aes_encryption.dart';

// SqliteAdapter is NOT exported here because `sqflite` depends on dart:ffi
// which is unavailable on web. Import it directly when targeting mobile:
//   import 'package:reliable/src/adapters/sqlite_adapter.dart';

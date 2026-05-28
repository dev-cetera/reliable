import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

import 'package:encrypt/encrypt.dart';
import '../reliable_core.dart';

/// Standard AES-256 Encryption implementation with random IV per call.
///
/// The [keyString] must be exactly 32 characters (256 bits).
/// Each encryption generates a unique random IV, prepended to the ciphertext.
class ReliableAesEncryption implements ReliableEncryption {
  static const int _kIvLengthBytes = 16;

  final Encrypter _encrypter;

  ReliableAesEncryption(String keyString)
      : _encrypter = Encrypter(AES(Key.fromUtf8(_validateKey(keyString))));

  static String _validateKey(String key) {
    if (key.length != 32) {
      throw ArgumentError(
        'ReliableAesEncryption: Key must be exactly 32 characters, '
        'got ${key.length}.',
      );
    }
    return key;
  }

  @override
  String get signature => 'aes_v2';

  @override
  String encrypt(String input) {
    final iv = IV(
      Uint8List.fromList(
        List<int>.generate(
          _kIvLengthBytes,
          (_) => Random.secure().nextInt(256),
        ),
      ),
    );
    final encrypted = _encrypter.encrypt(input, iv: iv);
    // Prepend IV bytes to ciphertext, then base64 the whole thing.
    final combined = Uint8List(_kIvLengthBytes + encrypted.bytes.length);
    combined.setRange(0, _kIvLengthBytes, iv.bytes);
    combined.setRange(_kIvLengthBytes, combined.length, encrypted.bytes);
    return base64.encode(combined);
  }

  @override
  String decrypt(String input) {
    final combined = base64.decode(input);
    final iv = IV(Uint8List.fromList(combined.sublist(0, _kIvLengthBytes)));
    final cipherBytes = combined.sublist(_kIvLengthBytes);
    return _encrypter.decrypt(
      Encrypted(Uint8List.fromList(cipherBytes)),
      iv: iv,
    );
  }
}

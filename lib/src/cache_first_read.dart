import 'dart:async';

import 'package:df_log/df_log.dart';
import 'reliable_core.dart';

/// Where the first answer of a [readCacheFirst] came from.
enum CacheFirstSource {
  /// The device's cached copy, shown at once; the network answer follows in
  /// [CacheFirstAnswer.refreshed].
  CACHE,

  /// Nothing usable was cached, so the read waited for the network (or, when
  /// that failed, for the cache's empty answer) like a network-first read.
  NETWORK,
}

/// What [readCacheFirst] reports once its first answer has been applied.
class CacheFirstAnswer {
  const CacheFirstAnswer({required this.source, required this.refreshed});

  final CacheFirstSource source;

  /// Completes when the background network answer has been applied, or has
  /// failed (already logged). Never throws. Already complete for a
  /// [CacheFirstSource.NETWORK] answer.
  final Future<void> refreshed;
}

/// Answers a read from the device at once, then refreshes it from the network
/// behind the screen.
///
/// [read] is the caller's own `ReliableRepository.fetch` / `fetchOne` call,
/// with the [CacheStrategy] left to this function. When the cache holds a usable answer
/// ([isUsable]), [apply] gets it straight away and this returns; the network
/// read then runs in the background and hands its answer to [apply] again
/// when it lands. So on lie-fi (connected, nothing answering) a screen shows
/// what the device already had instead of waiting out the request timeout.
///
/// With nothing usable cached (a first sign-in, or an empty list) it reads
/// network-first (`NETWORK_OR_ELSE_CACHE`) and applies that: there is nothing
/// faster to show.
///
/// [apply] may run twice and must make each answer the whole truth (replace
/// the shown state, never append to it). Ignoring an answer that lands after
/// the caller's session ended is the caller's job; [what] names the read in
/// the log line of a failed background apply.
Future<CacheFirstAnswer> readCacheFirst<T>({
  required String what,
  required Future<T> Function(CacheStrategy strategy) read,
  required bool Function(T cached) isUsable,
  required FutureOr<void> Function(T answer) apply,
}) async {
  final cached = await read(CacheStrategy.CACHE_ONLY);
  if (!isUsable(cached)) {
    await apply(await read(CacheStrategy.NETWORK_OR_ELSE_CACHE));
    return CacheFirstAnswer(
      source: CacheFirstSource.NETWORK,
      refreshed: Future.value(),
    );
  }
  await apply(cached);
  return CacheFirstAnswer(
    source: CacheFirstSource.CACHE,
    refreshed: _refresh(what: what, read: read, apply: apply),
  );
}

/// [readCacheFirst] for a list read (`repo.fetch`): a non-empty cached list
/// is usable.
Future<CacheFirstAnswer> readListCacheFirst({
  required String what,
  required Future<List<Map<String, dynamic>>> Function(CacheStrategy strategy)
      read,
  required FutureOr<void> Function(List<Map<String, dynamic>> answer) apply,
}) {
  return readCacheFirst(
    what: what,
    read: read,
    isUsable: (cached) => cached.isNotEmpty,
    apply: apply,
  );
}

/// [readCacheFirst] for a one-document read (`repo.fetchOne`): any cached
/// document is usable.
Future<CacheFirstAnswer> readOneCacheFirst({
  required String what,
  required Future<Map<String, dynamic>?> Function(CacheStrategy strategy) read,
  required FutureOr<void> Function(Map<String, dynamic>? answer) apply,
}) {
  return readCacheFirst(
    what: what,
    read: read,
    isUsable: (cached) => cached != null,
    apply: apply,
  );
}

Future<void> _refresh<T>({
  required String what,
  required Future<T> Function(CacheStrategy strategy) read,
  required FutureOr<void> Function(T answer) apply,
}) async {
  final T fresh;
  try {
    fresh = await read(CacheStrategy.NETWORK_ONLY);
  } catch (_) {
    // `ReliableRepository` has logged it (a warning for no answer, an error
    // for a bad one), and the cached answer is already showing.
    return;
  }
  try {
    await apply(fresh);
  } catch (e, stackTrace) {
    // A bug in the caller's apply, reached in the background where nobody
    // awaits it: logged as an error instead of vanishing.
    Log.err(
      'Background refresh of $what could not be applied: $e\n$stackTrace',
    );
  }
}

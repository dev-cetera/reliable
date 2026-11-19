/// One change the device made, waiting in the outbox until the server has
/// it.
///
/// [id] is minted on the device and is the request's idempotency key, so a
/// send whose answer was lost replays harmlessly. [kind] names the
/// `SyncMutationKind` that applies and sends it; [payload] is that kind's
/// own data, stored as given.
class SyncMutation {
  const SyncMutation({
    required this.id,
    required this.seq,
    required this.kind,
    required this.payload,
    required this.createdAt,
    required this.attempts,
    required this.isSent,
  });

  /// Device-minted id; also the idempotency key of every send.
  final String id;

  /// Position in the outbox: mutations reach the server in [seq] order.
  final int seq;

  /// The name of the `SyncMutationKind` that handles it.
  final String kind;

  /// The kind's own data.
  final Map<String, dynamic> payload;

  /// When the user made the change (UTC).
  final DateTime createdAt;

  /// Sends that got no answer or a server error so far.
  final int attempts;

  /// True once the server accepted it. A sent mutation stays in the view
  /// until a pull that started after the acceptance has landed, so the
  /// change never blinks out between the send and the pull that confirms
  /// it.
  final bool isSent;

  SyncMutation copyWith({int? attempts, bool? isSent}) => SyncMutation(
        id: id,
        seq: seq,
        kind: kind,
        payload: payload,
        createdAt: createdAt,
        attempts: attempts ?? this.attempts,
        isSent: isSent ?? this.isSent,
      );

  Map<String, dynamic> toMap() => {
        'id': id,
        'seq': seq,
        'kind': kind,
        'payload': payload,
        'created_at': createdAt.toUtc().toIso8601String(),
        'attempts': attempts,
        'is_sent': isSent,
      };

  /// Reads a stored mutation. A malformed one throws [FormatException]:
  /// the engine logs and drops it instead of stalling the outbox for good.
  factory SyncMutation.fromMap(Map<String, dynamic> map) {
    T read<T>(String key) {
      final value = map[key];
      if (value is T) return value;
      throw FormatException('Stored sync mutation has a bad "$key": $value');
    }

    final payload = read<Map<dynamic, dynamic>>('payload');
    return SyncMutation(
      id: read<String>('id'),
      seq: read<int>('seq'),
      kind: read<String>('kind'),
      payload: payload.cast<String, dynamic>(),
      createdAt: DateTime.parse(read<String>('created_at')).toUtc(),
      attempts: read<int>('attempts'),
      isSent: read<bool>('is_sent'),
    );
  }
}

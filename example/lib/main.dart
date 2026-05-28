//.title
// ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
//
// Copyright © dev-cetera.com & contributors.
//
// The use of this source code is governed by an MIT-style license described in
// the LICENSE file located in this project's root directory.
//
// See: https://opensource.org/license/mit
//
// ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
//.title~

import 'package:flutter/material.dart';
import 'package:reliable/reliable.dart';

// ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░

void main() {
  runApp(const ReliableExampleApp());
}

class ReliableExampleApp extends StatelessWidget {
  const ReliableExampleApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'reliable example',
      theme: ThemeData(useMaterial3: true, colorSchemeSeed: Colors.indigo),
      home: const TodoPage(),
    );
  }
}

// ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░

/// A fake in-memory backend. Toggle [isOnline] to simulate connectivity drops
/// so you can see writes pile up in the offline queue and drain when the
/// network comes back.
class _FakeServer implements NetworkAdapter {
  final List<Map<String, dynamic>> _store = [
    {'id': 'seed-1', 'title': 'Wash dishes'},
    {'id': 'seed-2', 'title': 'Walk the dog'},
  ];

  bool isOnline = true;

  @override
  Future<dynamic> request(
    String endpoint,
    RequestMethod method, {
    dynamic data,
    Map<String, dynamic>? queryParams,
    Map<String, String>? headers,
  }) async {
    await Future<void>.delayed(const Duration(milliseconds: 250));
    if (!isOnline) {
      throw const ReliableNetworkException(message: 'fake offline');
    }
    switch (method) {
      case RequestMethod.GET:
        return List.of(_store);
      case RequestMethod.POST:
        final payload = Map<String, dynamic>.from(data as Map);
        _store.add(payload);
        return payload;
      case RequestMethod.DELETE:
        final id = endpoint.split('/').last;
        _store.removeWhere((e) => e['id'] == id);
        return null;
      case RequestMethod.PUT:
      case RequestMethod.PATCH:
        return data;
    }
  }
}

// ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░

class TodoPage extends StatefulWidget {
  const TodoPage({super.key});

  @override
  State<TodoPage> createState() => _TodoPageState();
}

class _TodoPageState extends State<TodoPage> {
  final _FakeServer _server = _FakeServer();
  final SembastAdapter _storage = SembastAdapter(
    dbName: 'reliable_example.db',
  );
  late final ReliableRepository _repo = ReliableRepository(
    api: _server,
    storage: _storage,
  );

  List<Map<String, dynamic>> _todos = const [];
  int _pending = 0;
  bool _ready = false;

  @override
  void initState() {
    super.initState();
    _bootstrap();
  }

  Future<void> _bootstrap() async {
    await _storage.init(
      version: 1,
      onUpgrade: (db, oldVersion, newVersion) async {},
    );
    await _repo.initialize();
    setState(() => _ready = true);
    await _refresh();
  }

  Future<void> _refresh() async {
    final todos = await _repo.fetch('todos', '/todos');
    final pending = await _repo.getPendingCount();
    if (!mounted) return;
    setState(() {
      _todos = todos;
      _pending = pending;
    });
  }

  Future<void> _addTodo() async {
    await _repo.write(
      collection: 'todos',
      endpoint: '/todos',
      method: RequestMethod.POST,
      data: {'title': 'New todo @ ${DateTime.now().toIso8601String()}'},
    );
    await _refresh();
  }

  Future<void> _deleteTodo(String id) async {
    await _repo.delete(collection: 'todos', endpoint: '/todos', id: id);
    await _refresh();
  }

  void _toggleNetwork() {
    setState(() => _server.isOnline = !_server.isOnline);
    if (_server.isOnline) {
      // Coming back online — force the queue to drain immediately rather than
      // waiting for the next 5s heartbeat tick.
      _repo.syncNow().then((_) => _refresh());
    }
  }

  @override
  void dispose() {
    _repo.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    if (!_ready) {
      return const Scaffold(body: Center(child: CircularProgressIndicator()));
    }
    return Scaffold(
      appBar: AppBar(
        title: const Text('reliable example'),
        actions: [
          IconButton(
            tooltip: _server.isOnline ? 'Go offline' : 'Go online',
            icon: Icon(_server.isOnline ? Icons.wifi : Icons.wifi_off),
            onPressed: _toggleNetwork,
          ),
        ],
      ),
      body: Column(
        children: [
          Container(
            color: _server.isOnline
                ? Colors.green.withValues(alpha: 0.15)
                : Colors.orange.withValues(alpha: 0.2),
            padding: const EdgeInsets.all(12),
            child: Row(
              children: [
                Expanded(
                  child: Text(
                    _server.isOnline
                        ? 'Online — writes sync immediately.'
                        : 'Offline — writes queue locally.',
                  ),
                ),
                Text('Pending: $_pending'),
              ],
            ),
          ),
          Expanded(
            child: RefreshIndicator(
              onRefresh: _refresh,
              child: ListView.builder(
                itemCount: _todos.length,
                itemBuilder: (context, i) {
                  final todo = _todos[i];
                  final id = (todo['id'] ?? '').toString();
                  return ListTile(
                    title: Text((todo['title'] ?? '(no title)').toString()),
                    subtitle: Text(id),
                    trailing: IconButton(
                      icon: const Icon(Icons.delete_outline),
                      onPressed: () => _deleteTodo(id),
                    ),
                  );
                },
              ),
            ),
          ),
        ],
      ),
      floatingActionButton: FloatingActionButton(
        onPressed: _addTodo,
        child: const Icon(Icons.add),
      ),
    );
  }
}

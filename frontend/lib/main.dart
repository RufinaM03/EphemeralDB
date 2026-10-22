import 'dart:async';
import 'dart:convert';
import 'dart:js_interop';
import 'dart:math' as math;
import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;
import 'package:web/web.dart' as web;
import 'widgets.dart';

typedef Json = Map<String, dynamic>;
void main() => runApp(const ConsoleApp());

class ConsoleApp extends StatelessWidget {
  const ConsoleApp({super.key});
  @override
  Widget build(BuildContext context) => MaterialApp(
    title: 'EphemeralDB · Systems Lab',
    debugShowCheckedModeBanner: false,
    theme: ThemeData(
      useMaterial3: true,
      scaffoldBackgroundColor: paper,
      colorScheme: ColorScheme.fromSeed(
        seedColor: teal,
        primary: teal,
        surface: Colors.white,
      ),
      fontFamily: 'Arial',
      textTheme: const TextTheme(
        bodyMedium: TextStyle(color: ink, fontSize: 14),
      ),
      inputDecorationTheme: InputDecorationTheme(
        filled: true,
        fillColor: paper,
        border: OutlineInputBorder(
          borderRadius: BorderRadius.circular(9),
          borderSide: const BorderSide(color: edge),
        ),
        enabledBorder: OutlineInputBorder(
          borderRadius: BorderRadius.circular(9),
          borderSide: const BorderSide(color: edge),
        ),
      ),
      filledButtonTheme: FilledButtonThemeData(
        style: FilledButton.styleFrom(
          backgroundColor: ink,
          foregroundColor: Colors.white,
          padding: const EdgeInsets.symmetric(horizontal: 20, vertical: 18),
          shape: RoundedRectangleBorder(borderRadius: BorderRadius.circular(8)),
        ),
      ),
      cardTheme: CardThemeData(
        elevation: 0,
        color: Colors.white,
        margin: EdgeInsets.zero,
        shape: RoundedRectangleBorder(
          borderRadius: BorderRadius.circular(12),
          side: const BorderSide(color: edge),
        ),
      ),
    ),
    home: const Lab(),
  );
}

class Lab extends StatefulWidget {
  const Lab({super.key});
  @override
  State<Lab> createState() => _LabState();
}

class _LabState extends State<Lab> {
  final client = http.Client();
  final base = const String.fromEnvironment('API_BASE_URL');
  String token = '',
      backend = 'local',
      mode = 'batch',
      profile = 'balanced',
      scenario = 'cache-loss',
      table = 'entities';
  int page = 0, rowOffset = 0, rowTotal = 0;
  bool busy = false, polling = false;
  String? error, selectedId, tenantId;
  List<Json> runs = [], outcomes = [], tenants = [], rows = [];
  Json caps = {}, selected = {};
  final compared = <String>{};
  final fields = <String, TextEditingController>{
    'name': TextEditingController(text: 'Tenant workload'),
    'tenants': TextEditingController(text: '8'),
    'entities': TextEditingController(text: '50'),
    'rate': TextEditingController(text: '40'),
    'duration': TextEditingController(text: '10'),
    'warmup': TextEditingController(text: '2'),
    'concurrency': TextEditingController(text: '8'),
    'batch_window_ms': TextEditingController(text: '5'),
    'checkpoint_every': TextEditingController(text: '0'),
    'seed': TextEditingController(text: '42'),
  };
  Timer? timer;
  @override
  void initState() {
    super.initState();
    refresh();
    timer = Timer.periodic(const Duration(seconds: 2), (_) => refresh());
  }

  @override
  void dispose() {
    timer?.cancel();
    client.close();
    for (final field in fields.values) {
      field.dispose();
    }
    super.dispose();
  }

  Future<dynamic> api(String path, {Json? body}) async {
    final uri = Uri.parse('$base$path');
    final headers = {
      'Content-Type': 'application/json',
      if (token.isNotEmpty) 'Authorization': 'Bearer $token',
    };
    final response = body == null
        ? await client.get(uri, headers: headers)
        : await client.post(uri, headers: headers, body: jsonEncode(body));
    if (response.statusCode >= 400) {
      String message = response.body;
      try {
        message = jsonDecode(message)['detail'].toString();
      } catch (_) {}
      throw Exception('${response.statusCode}: $message');
    }
    return jsonDecode(response.body);
  }

  Future<void> refresh() async {
    if (polling) return;
    polling = true;
    try {
      final values = await Future.wait([
        api('/api/capabilities'),
        api('/api/runs'),
      ]);
      if (!mounted) return;
      caps = Json.from(values[0]);
      runs = (values[1] as List).map((value) => Json.from(value)).toList();
      selectedId ??= runs.isEmpty
          ? null
          : runs.firstWhere(
              (run) => run['kind'] == 'traffic',
              orElse: () => runs.first,
            )['id'];
      selected = runs.firstWhere(
        (run) => run['id'] == selectedId,
        orElse: () => {},
      );
      if (selectedId != null && selected['kind'] == 'traffic') {
        final value = await api('/api/runs/$selectedId/outcomes?limit=100');
        outcomes = (value['rows'] as List)
            .map((row) => Json.from(row))
            .toList();
      } else {
        outcomes = [];
      }
      if (mounted) setState(() => error = null);
    } catch (exception) {
      if (mounted) setState(() => error = exception.toString());
    } finally {
      polling = false;
    }
  }

  Future<void> action(Future<void> Function() operation) async {
    setState(() => busy = true);
    try {
      await operation();
      await refresh();
    } catch (exception) {
      if (mounted) setState(() => error = exception.toString());
    } finally {
      if (mounted) setState(() => busy = false);
    }
  }

  Future<void> startRun({bool quick = false}) => action(() async {
    final config = <String, dynamic>{
      'backend': backend,
      'mode': mode,
      'profile': profile,
    };
    for (final entry in fields.entries) {
      config[entry.key] = entry.key == 'name'
          ? entry.value.text
          : int.parse(entry.value.text);
    }
    if (quick) {
      config.addAll({
        'name': 'Local baseline · balanced',
        'backend': 'local',
        'tenants': 8,
        'rate': 40,
        'duration': 10,
        'warmup': 2,
      });
    }
    final run = await api('/api/runs', body: config);
    selectedId = run['id'];
    tenantId = null;
    if (mounted) setState(() => page = 0);
  });
  Future<void> loadTable({bool reset = false}) async {
    if (selectedId == null) return;
    if (reset) {
      tenantId = null;
      rowOffset = 0;
    }
    await action(() async {
      final result = await api('/api/runs/$selectedId/tenants');
      tenants = (result['tenants'] as List)
          .map((row) => Json.from(row))
          .toList();
      tenantId ??= tenants.isEmpty ? null : tenants.first['tenant_id'];
      if (tenantId == null) return;
      final data = await api(
        '/api/runs/$selectedId/tables/$table?tenant_id=$tenantId&offset=$rowOffset&limit=20',
      );
      rows = (data['rows'] as List).map((row) => Json.from(row)).toList();
      rowTotal = data['total'];
    });
  }

  Future<void> export(String artifact) => action(() async {
    final response = await client.get(
      Uri.parse('$base/api/runs/$selectedId/export?artifact=$artifact'),
      headers: {if (token.isNotEmpty) 'Authorization': 'Bearer $token'},
    );
    if (response.statusCode != 200) throw Exception('Artifact unavailable');
    final blob = web.Blob(
      [response.body.toJS].toJS,
      web.BlobPropertyBag(type: 'application/json'),
    );
    final url = web.URL.createObjectURL(blob);
    (web.HTMLAnchorElement()
          ..href = url
          ..download =
              '$selectedId-$artifact.${artifact == 'run' ? 'json' : 'jsonl'}')
        .click();
    web.URL.revokeObjectURL(url);
  });
  void inspect(String title, dynamic value) => showDialog<void>(
    context: context,
    builder: (context) => AlertDialog(
      title: Text(title),
      content: SizedBox(
        width: 750,
        child: SingleChildScrollView(
          child: SelectableText(
            const JsonEncoder.withIndent('  ').convert(value),
            style: const TextStyle(fontFamily: 'monospace', fontSize: 12),
          ),
        ),
      ),
      actions: [
        TextButton(
          onPressed: () => Navigator.pop(context),
          child: const Text('Close'),
        ),
      ],
    ),
  );
  Future<void> connect() async {
    final controller = TextEditingController(text: token);
    await showDialog<void>(
      context: context,
      builder: (context) => AlertDialog(
        title: const Text('Console connection'),
        content: SizedBox(
          width: 400,
          child: TextField(
            controller: controller,
            obscureText: true,
            decoration: const InputDecoration(
              labelText: 'Console token',
              helperText: 'Empty for local-only access. Stored in memory.',
            ),
          ),
        ),
        actions: [
          TextButton(
            onPressed: () {
              token = controller.text;
              Navigator.pop(context);
              refresh();
            },
            child: const Text('Connect'),
          ),
        ],
      ),
    );
    controller.dispose();
  }

  void navigate(int index) {
    setState(() => page = index);
    if (index == 1) loadTable();
  }

  @override
  Widget build(BuildContext context) {
    final wide = MediaQuery.sizeOf(context).width > 1000;
    return Scaffold(
      body: Row(
        children: [
          if (wide) sidebar(),
          Expanded(
            child: Column(
              children: [
                Container(
                  height: 70,
                  decoration: const BoxDecoration(
                    color: Colors.white,
                    border: Border(bottom: BorderSide(color: edge)),
                  ),
                  padding: const EdgeInsets.symmetric(horizontal: 28),
                  child: Row(
                    children: [
                      if (!wide)
                        PopupMenuButton<int>(
                          icon: const Icon(Icons.menu),
                          onSelected: navigate,
                          itemBuilder: (_) => [
                            for (var i = 0; i < 5; i++)
                              PopupMenuItem(value: i, child: Text(pages[i])),
                          ],
                        ),
                      const Text(
                        'Workspace',
                        style: TextStyle(color: muted, fontSize: 12),
                      ),
                      const Padding(
                        padding: EdgeInsets.symmetric(horizontal: 13),
                        child: Text('/', style: TextStyle(color: edge)),
                      ),
                      Text(
                        pages[page],
                        style: const TextStyle(
                          fontSize: 12,
                          fontWeight: FontWeight.w600,
                        ),
                      ),
                      const Spacer(),
                      badge(
                        error == null ? 'API CONNECTED' : 'API OFFLINE',
                        error == null ? teal : Colors.deepOrange,
                      ),
                      const SizedBox(width: 12),
                      IconButton(
                        onPressed: connect,
                        tooltip: 'Console connection',
                        icon: const Icon(Icons.settings_outlined, size: 20),
                      ),
                    ],
                  ),
                ),
                if (busy) const LinearProgressIndicator(minHeight: 2),
                if (error != null)
                  Container(
                    width: double.infinity,
                    color: const Color(0xFFFFE8DF),
                    padding: const EdgeInsets.symmetric(
                      horizontal: 28,
                      vertical: 10,
                    ),
                    child: Row(
                      children: [
                        const Icon(Icons.info_outline, size: 18),
                        const SizedBox(width: 10),
                        Expanded(child: Text(error!, maxLines: 3)),
                        TextButton(
                          onPressed: connect,
                          child: const Text('Connection'),
                        ),
                      ],
                    ),
                  ),
                Expanded(
                  child: SingleChildScrollView(
                    padding: EdgeInsets.all(wide ? 32 : 18),
                    child: [
                      overview,
                      database,
                      traffic,
                      failures,
                      comparisons,
                    ][page](),
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  Widget sidebar() => Container(
    width: 224,
    color: ink,
    padding: const EdgeInsets.all(22),
    child: Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        const SizedBox(height: 15),
        Row(
          children: [
            Container(
              width: 30,
              height: 32,
              decoration: BoxDecoration(
                color: amber,
                borderRadius: BorderRadius.circular(7),
              ),
              child: const Icon(Icons.storage_rounded, size: 20, color: ink),
            ),
            const SizedBox(width: 10),
            const Text(
              'ephemeraldb',
              style: TextStyle(
                color: Colors.white,
                fontSize: 21,
                fontWeight: FontWeight.w700,
                letterSpacing: -.7,
              ),
            ),
          ],
        ),
        const Padding(
          padding: EdgeInsets.only(top: 12, bottom: 44),
          child: Text(
            'DATABASE SYSTEMS LAB',
            style: TextStyle(
              color: Color(0xFF8EA7AD),
              fontSize: 10,
              letterSpacing: 1.8,
            ),
          ),
        ),
        const Text(
          'WORKSPACE',
          style: TextStyle(
            color: Color(0xFF8EA7AD),
            fontSize: 10,
            letterSpacing: 1.6,
          ),
        ),
        const SizedBox(height: 15),
        for (var index = 0; index < 5; index++)
          Padding(
            padding: const EdgeInsets.only(bottom: 7),
            child: Material(
              color: page == index
                  ? const Color(0xFF2C414A)
                  : Colors.transparent,
              borderRadius: BorderRadius.circular(7),
              child: InkWell(
                borderRadius: BorderRadius.circular(7),
                onTap: () => navigate(index),
                child: Padding(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 12,
                    vertical: 14,
                  ),
                  child: Row(
                    children: [
                      Icon(
                        [
                          Icons.space_dashboard_outlined,
                          Icons.table_chart_outlined,
                          Icons.show_chart,
                          Icons.bolt_outlined,
                          Icons.compare_arrows,
                        ][index],
                        size: 19,
                        color: page == index ? amber : const Color(0xFFAFC0C4),
                      ),
                      const SizedBox(width: 12),
                      Text(
                        pages[index],
                        style: TextStyle(
                          fontSize: 13,
                          fontWeight: page == index
                              ? FontWeight.w600
                              : FontWeight.w400,
                          color: page == index
                              ? Colors.white
                              : const Color(0xFFAFC0C4),
                        ),
                      ),
                    ],
                  ),
                ),
              ),
            ),
          ),
        const Spacer(),
        Container(
          padding: const EdgeInsets.all(14),
          decoration: BoxDecoration(
            border: Border.all(color: const Color(0xFF3D5159)),
            borderRadius: BorderRadius.circular(9),
          ),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              const Row(
                children: [
                  Icon(Icons.science_outlined, size: 17, color: amber),
                  SizedBox(width: 8),
                  Text(
                    'Evidence first',
                    style: TextStyle(color: Colors.white, fontSize: 12),
                  ),
                ],
              ),
              const SizedBox(height: 9),
              const Text(
                'Every chart comes from an execution. Cloud runs require a configured AWS environment.',
                style: TextStyle(
                  color: Color(0xFFAFC0C4),
                  height: 1.6,
                  fontSize: 11,
                ),
              ),
              const SizedBox(height: 12),
              Text(
                caps['region'] ?? 'ap-south-1',
                style: const TextStyle(
                  color: amber,
                  fontFamily: 'monospace',
                  fontSize: 11,
                ),
              ),
            ],
          ),
        ),
        const SizedBox(height: 18),
        const Text(
          'EXPERIMENTAL INFRASTRUCTURE',
          style: TextStyle(
            fontSize: 9,
            letterSpacing: 1,
            color: Color(0xFF7F989F),
          ),
        ),
      ],
    ),
  );
  Widget runPicker() => DropdownButtonHideUnderline(
    child: DropdownButton<String>(
      value: selectedId,
      hint: const Text('Select an experiment', style: TextStyle(fontSize: 12)),
      items: runs
          .map(
            (run) => DropdownMenuItem<String>(
              value: run['id'],
              child: SizedBox(
                width: 210,
                child: Text(
                  run['name'],
                  overflow: TextOverflow.ellipsis,
                  style: const TextStyle(fontSize: 12),
                ),
              ),
            ),
          )
          .toList(),
      onChanged: (value) {
        setState(() {
          selectedId = value;
          tenantId = null;
          rows = [];
        });
        refresh();
        if (page == 1) loadTable(reset: true);
      },
    ),
  );
  Widget readiness(String key) => Padding(
    padding: const EdgeInsets.only(bottom: 16),
    child: Row(
      children: [
        Icon(
          caps[key]?['configured'] == true
              ? Icons.check_circle
              : Icons.radio_button_unchecked,
          color: caps[key]?['configured'] == true ? teal : muted,
          size: 16,
        ),
        const SizedBox(width: 10),
        Expanded(
          child: Text(
            caps[key]?['label'] ?? key,
            style: const TextStyle(fontSize: 11),
          ),
        ),
        Text(
          caps[key]?['configured'] == true ? 'Configured' : 'Not configured',
          style: const TextStyle(color: muted, fontSize: 10),
        ),
      ],
    ),
  );
  Widget overview() {
    final metrics = Json.from(selected['metrics'] ?? {});
    final series = (selected['series'] as List? ?? [])
        .map((value) => Json.from(value))
        .toList();
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        heading(
          'OBSERVE · EXECUTE · VERIFY',
          'A clear view of every commit.',
          'Explore tenant databases, exercise the runtime, and measure what survives.',
          action: FilledButton.icon(
            onPressed: busy ? null : () => startRun(quick: true),
            icon: const Icon(Icons.play_arrow, size: 18),
            label: const Text('Run local baseline'),
          ),
        ),
        Wrap(
          spacing: 14,
          crossAxisAlignment: WrapCrossAlignment.center,
          children: [
            badge(selected['evidence_scope'] ?? 'NO EXECUTION SELECTED', teal),
            if (selected['status'] != null)
              badge(
                selected['status'],
                selected['status'] == 'failed' ? Colors.deepOrange : muted,
              ),
            runPicker(),
          ],
        ),
        const SizedBox(height: 20),
        LayoutBuilder(
          builder: (_, constraints) {
            final width = constraints.maxWidth < 500
                ? (constraints.maxWidth - 15) / 2
                : math.max(210.0, (constraints.maxWidth - 45) / 4);
            return Wrap(
              spacing: 15,
              runSpacing: 15,
              children: [
                stat(
                  width,
                  'Successful throughput',
                  metrics['goodput'],
                  'operations / second · measurement only',
                  Icons.speed,
                ),
                stat(
                  width,
                  'p95 end-to-end latency',
                  metrics['p95_ms'],
                  'milliseconds · successful operations',
                  Icons.timer_outlined,
                ),
                stat(
                  width,
                  'Successful operations',
                  metrics['successful'],
                  '${metrics['failed'] ?? 0} failed · ${metrics['dropped'] ?? 0} overload drops',
                  Icons.check_circle_outline,
                ),
                stat(
                  width,
                  'Tenant databases',
                  selected['config']?['tenants'],
                  'independent state · shared persistence',
                  Icons.dns_outlined,
                ),
              ],
            );
          },
        ),
        const SizedBox(height: 22),
        panel(
          'Execution throughput',
          Column(
            children: [
              SizedBox(
                height: 190,
                width: double.infinity,
                child: series.isEmpty
                    ? empty(
                        'Start an experiment to see measured traffic.',
                        Icons.show_chart,
                      )
                    : CustomPaint(painter: TrafficPainter(series)),
              ),
              const SizedBox(height: 14),
              const Row(
                children: [
                  Icon(Icons.circle, size: 7, color: teal),
                  SizedBox(width: 7),
                  Text(
                    'Successful operations per second',
                    style: TextStyle(color: muted, fontSize: 10),
                  ),
                  Spacer(),
                  Text(
                    'Measurement time →',
                    style: TextStyle(color: muted, fontSize: 10),
                  ),
                ],
              ),
            ],
          ),
          subtitle:
              'Real completions by second. Warm-up is excluded; dropped work remains visible in run metrics.',
          trailing: badge(
            selected['status'] == 'running' ? 'LIVE · 2s refresh' : 'RECORDED',
            teal,
          ),
        ),
        const SizedBox(height: 22),
        LayoutBuilder(
          builder: (_, constraints) => Wrap(
            spacing: 20,
            runSpacing: 20,
            children: [
              SizedBox(
                width: constraints.maxWidth > 850
                    ? constraints.maxWidth * .58
                    : constraints.maxWidth,
                child: panel(
                  'Commit path',
                  Column(
                    children: [
                      flow(
                        '01',
                        'Prepare tenant mutation',
                        'Per-tenant ordering and request identity',
                        Icons.edit_note,
                      ),
                      flow(
                        '02',
                        'Persist WAL payload',
                        '${selected['config']?['mode'] ?? 'Single / batched'} · ${selected['evidence_scope'] ?? 'select an execution'}',
                        Icons.cloud_upload_outlined,
                      ),
                      flow(
                        '03',
                        selected['config']?['backend'] == 's3-express'
                            ? 'Accept through authority CAS'
                            : 'Apply local state',
                        selected['config']?['backend'] == 's3-express'
                            ? 'S3 Standard · fenced control head'
                            : 'AWS fenced runs add an authority decision',
                        Icons.verified_outlined,
                      ),
                      flow(
                        '04',
                        'Acknowledge to caller',
                        'Inspect exact outcomes in the transaction table',
                        Icons.check_circle_outline,
                      ),
                    ],
                  ),
                  subtitle: 'Protocol view; this is not a live packet trace.',
                ),
              ),
              SizedBox(
                width: constraints.maxWidth > 850
                    ? constraints.maxWidth * .39
                    : constraints.maxWidth,
                child: panel(
                  'Environment readiness',
                  Column(
                    children: [
                      for (final key in [
                        'local',
                        's3-express',
                        'http',
                        'aurora',
                        'fis',
                      ])
                        readiness(key),
                      const SizedBox(height: 12),
                      const Text(
                        'Configured does not establish AWS connectivity. No cloud results are generated locally.',
                        style: TextStyle(
                          color: muted,
                          fontSize: 11,
                          height: 1.6,
                        ),
                      ),
                    ],
                  ),
                  subtitle:
                      'Local execution is ready without cloud credentials.',
                ),
              ),
            ],
          ),
        ),
        const SizedBox(height: 22),
        runHistory(),
      ],
    );
  }

  Widget runHistory() => panel(
    'Experiment history',
    runs.isEmpty
        ? empty(
            'No executions yet. Run a local baseline to create your first evidence.',
            Icons.history,
          )
        : SingleChildScrollView(
            scrollDirection: Axis.horizontal,
            child: DataTable(
              headingTextStyle: const TextStyle(
                color: muted,
                fontSize: 10,
                fontWeight: FontWeight.w600,
              ),
              dataTextStyle: const TextStyle(color: ink, fontSize: 11),
              columns: const [
                DataColumn(label: Text('EXPERIMENT')),
                DataColumn(label: Text('BACKEND / SCOPE')),
                DataColumn(label: Text('STATUS')),
                DataColumn(label: Text('GOODPUT')),
                DataColumn(label: Text('P95')),
                DataColumn(label: Text('INSPECT')),
              ],
              rows: runs
                  .take(12)
                  .map(
                    (run) => DataRow(
                      selected: run['id'] == selectedId,
                      cells: [
                        DataCell(Text(run['name'])),
                        DataCell(
                          Text(
                            run['config']['backend'] ?? 'offline process proof',
                          ),
                        ),
                        DataCell(
                          badge(
                            run['status'],
                            run['status'] == 'failed'
                                ? Colors.deepOrange
                                : teal,
                          ),
                        ),
                        DataCell(Text('${run['metrics']?['goodput'] ?? '—'}')),
                        DataCell(
                          Text('${run['metrics']?['p95_ms'] ?? '—'} ms'),
                        ),
                        DataCell(
                          IconButton(
                            icon: const Icon(Icons.arrow_outward, size: 16),
                            onPressed: () {
                              selectedId = run['id'];
                              refresh();
                              inspect('Execution manifest', run);
                            },
                          ),
                        ),
                      ],
                    ),
                  )
                  .toList(),
            ),
          ),
    trailing: IconButton(
      onPressed: refresh,
      icon: const Icon(Icons.refresh, size: 18),
    ),
  );
  Widget dataGrid(List<Json> data, {List<String>? keys}) {
    if (data.isEmpty) {
      return empty(
        'No records available for this selection.',
        Icons.table_rows_outlined,
      );
    }
    final columns = keys ?? data.first.keys.toList();
    return SizedBox(
      width: double.infinity,
      child: SingleChildScrollView(
        scrollDirection: Axis.horizontal,
        child: DataTable(
          headingTextStyle: const TextStyle(
            color: muted,
            fontWeight: FontWeight.w600,
            fontSize: 10,
          ),
          dataTextStyle: const TextStyle(
            color: ink,
            fontFamily: 'monospace',
            fontSize: 11,
          ),
          columns: columns.map((key) => DataColumn(label: Text(key))).toList(),
          rows: data
              .map(
                (row) => DataRow(
                  cells: columns
                      .map(
                        (key) => DataCell(
                          ConstrainedBox(
                            constraints: const BoxConstraints(maxWidth: 190),
                            child: Text(
                              '${row[key] ?? '—'}',
                              overflow: TextOverflow.ellipsis,
                            ),
                          ),
                          onTap: () => inspect('Record details', row),
                        ),
                      )
                      .toList(),
                ),
              )
              .toList(),
        ),
      ),
    );
  }

  Widget database() => Column(
    crossAxisAlignment: CrossAxisAlignment.start,
    children: [
      heading(
        'INSPECT THE STATE',
        'Database explorer',
        'Browse tenant-local cache snapshots and inspect actual transaction outcomes.',
      ),
      Wrap(
        spacing: 18,
        crossAxisAlignment: WrapCrossAlignment.center,
        children: [
          runPicker(),
          DropdownButton<String>(
            value: tenantId,
            hint: const Text('Select tenant'),
            items: tenants
                .map(
                  (tenant) => DropdownMenuItem<String>(
                    value: tenant['tenant_id'],
                    child: Text(
                      'tenant · ${tenant['tenant_id'].toString().substring(0, 8)}',
                      style: const TextStyle(fontSize: 12),
                    ),
                  ),
                )
                .toList(),
            onChanged: (value) {
              tenantId = value;
              rowOffset = 0;
              loadTable();
            },
          ),
          DropdownButton<String>(
            value: table,
            items:
                [
                      'tenants',
                      'entities',
                      'entity_versions',
                      'relationships',
                      'metadata',
                      'transactions',
                      'transaction_items',
                      'audit_log',
                    ]
                    .map(
                      (name) => DropdownMenuItem(
                        value: name,
                        child: Text(name, style: const TextStyle(fontSize: 12)),
                      ),
                    )
                    .toList(),
            onChanged: (value) {
              table = value!;
              rowOffset = 0;
              loadTable();
            },
          ),
          OutlinedButton.icon(
            onPressed: () => loadTable(),
            icon: const Icon(Icons.refresh, size: 16),
            label: const Text('Refresh snapshot'),
          ),
        ],
      ),
      const SizedBox(height: 18),
      panel(
        table,
        Column(
          children: [
            dataGrid(rows),
            const SizedBox(height: 12),
            Row(
              children: [
                Text(
                  '$rowTotal rows · offset $rowOffset',
                  style: const TextStyle(color: muted, fontSize: 11),
                ),
                const Spacer(),
                IconButton(
                  onPressed: rowOffset > 0
                      ? () {
                          rowOffset = math.max(0, rowOffset - 20);
                          loadTable();
                        }
                      : null,
                  icon: const Icon(Icons.chevron_left),
                ),
                IconButton(
                  onPressed: rowOffset + 20 < rowTotal
                      ? () {
                          rowOffset += 20;
                          loadTable();
                        }
                      : null,
                  icon: const Icon(Icons.chevron_right),
                ),
              ],
            ),
          ],
        ),
        subtitle:
            'Read-only compute-local state. Remote execution outcomes appear below.',
      ),
      const SizedBox(height: 22),
      panel(
        'Transaction inspector',
        dataGrid(
          outcomes,
          keys: [
            'operation_id',
            'operation_type',
            'success',
            'tenant_state',
            'durability_outcome',
            'end_to_end_ms',
          ],
        ),
        subtitle:
            'First 100 outcomes, including warm-up. Click a row for WAL locators, authority decisions, and timings.',
        trailing: TextButton(
          onPressed: selectedId == null ? null : () => export('outcomes'),
          child: const Text('Export all outcomes'),
        ),
      ),
    ],
  );
  Widget dropdown(
    String label,
    String value,
    List<String> values,
    void Function(String) change,
  ) => SizedBox(
    width: 260,
    child: DropdownButtonFormField<String>(
      initialValue: value,
      decoration: InputDecoration(labelText: label),
      items: values
          .map(
            (item) => DropdownMenuItem(
              value: item,
              child: Text(item, style: const TextStyle(fontSize: 13)),
            ),
          )
          .toList(),
      onChanged: (item) => setState(() => change(item!)),
    ),
  );
  Widget traffic() => Column(
    crossAxisAlignment: CrossAxisAlignment.start,
    children: [
      heading(
        'DESIGN AN EXPERIMENT',
        'Traffic lab',
        'Replay deterministic operations through a bounded, rate-controlled runner.',
      ),
      panel(
        'Workload configuration',
        Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Wrap(
              spacing: 20,
              runSpacing: 22,
              children: [
                dropdown('Execution backend', backend, [
                  'local',
                  's3-express',
                  'aurora',
                  'http',
                ], (value) => backend = value),
                dropdown('Persistence mode', mode, [
                  'single',
                  'batch',
                ], (value) => mode = value),
                dropdown('Workload profile', profile, [
                  'balanced',
                  'read-heavy',
                  'write-heavy',
                  'hot-contention',
                  'multi-tenant-skew',
                ], (value) => profile = value),
                for (final entry in fields.entries)
                  SizedBox(
                    width: 260,
                    child: TextField(
                      controller: entry.value,
                      keyboardType: entry.key == 'name'
                          ? TextInputType.text
                          : TextInputType.number,
                      decoration: InputDecoration(
                        labelText: {
                          'name': 'Experiment name',
                          'tenants': 'Tenant databases',
                          'entities': 'Entities per tenant',
                          'rate': 'Target operations / second',
                          'duration': 'Measurement duration (seconds)',
                          'warmup': 'Warm-up (seconds)',
                          'concurrency': 'Concurrent workers',
                          'batch_window_ms': 'Batch window (milliseconds)',
                          'checkpoint_every':
                              'Checkpoint every N writes (0 = off)',
                          'seed': 'Dataset and workload seed',
                        }[entry.key],
                      ),
                    ),
                  ),
              ],
            ),
            const SizedBox(height: 25),
            const Text(
              'Warm-up drains before measurement. Saturated admission drops work explicitly. Failed and duplicate operations do not count as successful throughput.',
              style: TextStyle(color: muted, fontSize: 12, height: 1.7),
            ),
            const SizedBox(height: 20),
            Wrap(
              spacing: 15,
              crossAxisAlignment: WrapCrossAlignment.center,
              children: [
                FilledButton.icon(
                  onPressed: busy || caps[backend]?['configured'] != true
                      ? null
                      : () => startRun(),
                  icon: const Icon(Icons.play_arrow, size: 18),
                  label: const Text('Start experiment'),
                ),
                badge(
                  backend == 'local'
                      ? 'LOCAL · NO AWS CALLS'
                      : 'LIVE CLOUD EXECUTION',
                  backend == 'local' ? teal : Colors.deepOrange,
                ),
              ],
            ),
          ],
        ),
        subtitle:
            'Each run gets a fresh dataset and persistence namespace. Cloud runs retain evidence and may incur charges.',
      ),
      const SizedBox(height: 22),
      panel(
        'Active execution',
        Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            runPicker(),
            const SizedBox(height: 12),
            Text(
              '${selected['status'] ?? 'No experiment selected'} · ${selected['processed'] ?? 0} outcomes recorded',
              style: const TextStyle(color: muted),
            ),
            const SizedBox(height: 12),
            Wrap(
              spacing: 12,
              children: [
                OutlinedButton(
                  onPressed:
                      selectedId == null ||
                          !['running', 'queued'].contains(selected['status'])
                      ? null
                      : () => action(() async {
                          await api('/api/runs/$selectedId/cancel', body: {});
                        }),
                  child: const Text('Stop admission & drain'),
                ),
                TextButton(
                  onPressed: selectedId == null ? null : () => export('run'),
                  child: const Text('Export manifest'),
                ),
                TextButton(
                  onPressed: selectedId == null
                      ? null
                      : () => export('workload'),
                  child: const Text('Export workload'),
                ),
              ],
            ),
          ],
        ),
      ),
    ],
  );
  Widget failures() => Column(
    crossAxisAlignment: CrossAxisAlignment.start,
    children: [
      heading(
        'BREAK · RECOVER · VERIFY',
        'Failure lab',
        'Exercise actual process failures and inspect retained correctness evidence.',
      ),
      panel(
        'Isolated recovery proof',
        Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Wrap(
              spacing: 20,
              runSpacing: 20,
              children: [
                dropdown('Failure scenario', scenario, [
                  'cache-loss',
                  'ack-loss',
                  'stale-owner',
                ], (value) => scenario = value),
                dropdown('WAL mode', mode, [
                  'single',
                  'batch',
                ], (value) => mode = value),
              ],
            ),
            const SizedBox(height: 22),
            const Text(
              'Cache loss kills a live child process and deletes its cache. ACK loss verifies retry identity. Stale-owner resumption tests fencing after takeover. These bounded proofs use an external local authority store.',
              style: TextStyle(color: muted, fontSize: 12, height: 1.8),
            ),
            const SizedBox(height: 20),
            FilledButton.icon(
              onPressed: busy
                  ? null
                  : () => action(() async {
                      final run = await api(
                        '/api/failures',
                        body: {'scenario': scenario, 'mode': mode},
                      );
                      selectedId = run['id'];
                    }),
              icon: const Icon(Icons.bolt, size: 18),
              label: const Text('Execute recovery proof'),
            ),
          ],
        ),
        trailing: badge('OFFLINE PROCESS PROOF', teal),
      ),
      const SizedBox(height: 22),
      panel(
        'Proof evidence',
        Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            runPicker(),
            const SizedBox(height: 15),
            if (selected['evidence'] != null)
              SelectableText(
                const JsonEncoder.withIndent(
                  '  ',
                ).convert(selected['evidence']),
                style: const TextStyle(
                  fontFamily: 'monospace',
                  fontSize: 11,
                  height: 1.6,
                ),
              )
            else
              empty(
                selected['status'] == 'running'
                    ? 'The process proof is running. Evidence appears on completion.'
                    : 'Select a completed failure proof to inspect its evidence.',
                Icons.fact_check_outlined,
              ),
          ],
        ),
      ),
      const SizedBox(height: 22),
      panel(
        'AWS infrastructure faults',
        Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            readiness('fis'),
            const Text(
              'Only allowlisted AWS FIS templates with a CloudWatch alarm stop condition can run. Configure dedicated EKS/EC2 targets and permissions before enabling cloud fault execution.',
              style: TextStyle(color: muted, height: 1.8, fontSize: 12),
            ),
            for (final id in caps['fis_templates'] ?? [])
              Padding(
                padding: const EdgeInsets.only(top: 12),
                child: OutlinedButton.icon(
                  onPressed: busy
                      ? null
                      : () => action(() async {
                          final result = await api(
                            '/api/fis/start',
                            body: {'template_id': id},
                          );
                          if (mounted) {
                            inspect('AWS FIS experiment started', result);
                          }
                        }),
                  icon: const Icon(Icons.cloud_outlined),
                  label: Text('Execute $id'),
                ),
              ),
          ],
        ),
      ),
    ],
  );
  Widget comparisons() {
    final candidates = runs
        .where(
          (run) => run['kind'] == 'traffic' && run['status'] == 'completed',
        )
        .toList();
    final chosen = candidates
        .where((run) => compared.contains(run['id']))
        .toList();
    final identities = chosen
        .map((run) => '${run['dataset_checksum']}:${run['workload_checksum']}')
        .toSet();
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        heading(
          'COMPARE THE EVIDENCE',
          'Architecture comparisons',
          'Match workload identity before drawing performance conclusions.',
        ),
        panel(
          'Select executions',
          candidates.isEmpty
              ? empty(
                  'Complete two traffic experiments to compare measured results.',
                  Icons.compare_arrows,
                )
              : Column(
                  children: [
                    for (final run in candidates)
                      CheckboxListTile(
                        contentPadding: EdgeInsets.zero,
                        title: Text(
                          run['name'],
                          style: const TextStyle(fontSize: 13),
                        ),
                        subtitle: Text(
                          '${run['config']['backend']} · ${run['config']['mode']} · ${run['config']['tenants']} tenants · ${run['config']['rate']} ops/s',
                          style: const TextStyle(fontSize: 11, color: muted),
                        ),
                        value: compared.contains(run['id']),
                        onChanged: (value) => setState(() {
                          if (value == true) {
                            compared.add(run['id']);
                          } else {
                            compared.remove(run['id']);
                          }
                        }),
                      ),
                  ],
                ),
        ),
        const SizedBox(height: 22),
        if (chosen.length > 1) ...[
          badge(
            identities.length == 1
                ? 'MATCHED DATASET + WORKLOAD'
                : 'DIFFERENT WORKLOADS · EXPLORATORY COMPARISON',
            identities.length == 1 ? teal : Colors.deepOrange,
          ),
          const SizedBox(height: 18),
          panel(
            'Measured results',
            dataGrid(
              chosen
                  .map(
                    (run) => <String, dynamic>{
                      'experiment': run['name'],
                      'backend': run['config']['backend'],
                      'mode': run['config']['mode'],
                      'goodput': run['metrics']['goodput'],
                      'p50_ms': run['metrics']['p50_ms'],
                      'p95_ms': run['metrics']['p95_ms'],
                      'p99_ms': run['metrics']['p99_ms'],
                      'failed': run['metrics']['failed'],
                      'dropped': run['metrics']['dropped'],
                    },
                  )
                  .toList(),
            ),
          ),
          const SizedBox(height: 22),
          panel(
            'Successful throughput · operations / second',
            Column(
              children: [
                for (final run in chosen)
                  Padding(
                    padding: const EdgeInsets.only(bottom: 16),
                    child: Row(
                      children: [
                        SizedBox(
                          width: 190,
                          child: Text(
                            run['name'],
                            overflow: TextOverflow.ellipsis,
                            style: const TextStyle(fontSize: 12),
                          ),
                        ),
                        Expanded(
                          child: LinearProgressIndicator(
                            minHeight: 18,
                            borderRadius: BorderRadius.circular(3),
                            color: teal,
                            backgroundColor: paper,
                            value:
                                (run['metrics']['goodput'] as num).toDouble() /
                                math.max(
                                  1,
                                  chosen
                                      .map(
                                        (item) =>
                                            (item['metrics']['goodput'] as num)
                                                .toDouble(),
                                      )
                                      .reduce(math.max),
                                ),
                          ),
                        ),
                        SizedBox(
                          width: 80,
                          child: Text(
                            '${run['metrics']['goodput']}',
                            textAlign: TextAlign.right,
                            style: const TextStyle(
                              fontFamily: 'monospace',
                              fontSize: 12,
                            ),
                          ),
                        ),
                      ],
                    ),
                  ),
              ],
            ),
          ),
          const SizedBox(height: 18),
          const Text(
            'Cost is unavailable until independently measured. Local results are not AWS performance evidence. Repeat matched experiments; a single run does not establish an architectural advantage.',
            style: TextStyle(color: muted, height: 1.8, fontSize: 12),
          ),
        ],
      ],
    );
  }
}

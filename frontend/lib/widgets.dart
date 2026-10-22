import 'dart:math' as math;
import 'package:flutter/material.dart';

const ink = Color(0xFF172A35),
    muted = Color(0xFF697B83),
    paper = Color(0xFFF5F7F6),
    teal = Color(0xFF087F72),
    amber = Color(0xFFE9B55B),
    edge = Color(0xFFE0E7E4);
const pages = [
  'Overview',
  'Database explorer',
  'Traffic lab',
  'Failure lab',
  'Comparisons',
];
Widget badge(String text, Color color) => Container(
  padding: const EdgeInsets.symmetric(horizontal: 9, vertical: 5),
  decoration: BoxDecoration(
    color: color.withValues(alpha: .09),
    borderRadius: BorderRadius.circular(5),
  ),
  child: Text(
    text.toUpperCase(),
    style: TextStyle(
      color: color,
      fontSize: 10,
      fontWeight: FontWeight.w700,
      letterSpacing: .5,
    ),
  ),
);
Widget heading(
  String eyebrow,
  String title,
  String subtitle, {
  Widget? action,
}) => Padding(
  padding: const EdgeInsets.only(bottom: 27),
  child: Wrap(
    alignment: WrapAlignment.spaceBetween,
    crossAxisAlignment: WrapCrossAlignment.center,
    spacing: 25,
    runSpacing: 16,
    children: [
      Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            eyebrow,
            style: const TextStyle(
              color: teal,
              fontSize: 10,
              fontWeight: FontWeight.w700,
              letterSpacing: 2,
            ),
          ),
          const SizedBox(height: 10),
          Text(
            title,
            style: const TextStyle(
              color: ink,
              fontWeight: FontWeight.w700,
              fontSize: 30,
              letterSpacing: -1,
            ),
          ),
          const SizedBox(height: 9),
          Text(
            subtitle,
            style: const TextStyle(color: muted, fontSize: 12, height: 1.6),
          ),
        ],
      ),
      ?action,
    ],
  ),
);
Widget panel(
  String title,
  Widget child, {
  String? subtitle,
  Widget? trailing,
}) => Card(
  child: Padding(
    padding: const EdgeInsets.all(22),
    child: Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Row(
          children: [
            Expanded(
              child: Text(
                title,
                style: const TextStyle(
                  fontWeight: FontWeight.w600,
                  fontSize: 15,
                ),
              ),
            ),
            ?trailing,
          ],
        ),
        if (subtitle != null)
          Padding(
            padding: const EdgeInsets.only(top: 7),
            child: Text(
              subtitle,
              style: const TextStyle(fontSize: 11, color: muted, height: 1.5),
            ),
          ),
        const SizedBox(height: 20),
        child,
      ],
    ),
  ),
);
Widget stat(
  double width,
  String label,
  dynamic value,
  String footnote,
  IconData icon,
) => SizedBox(
  width: width,
  child: Card(
    child: Padding(
      padding: const EdgeInsets.all(20),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Row(
            children: [
              Expanded(
                child: Text(
                  label,
                  style: const TextStyle(color: muted, fontSize: 12),
                ),
              ),
              Icon(icon, color: teal, size: 18),
            ],
          ),
          const SizedBox(height: 18),
          Text(
            value == null ? '—' : '$value',
            style: const TextStyle(
              fontSize: 30,
              fontWeight: FontWeight.w600,
              letterSpacing: -1,
            ),
          ),
          const SizedBox(height: 9),
          Text(footnote, style: const TextStyle(color: muted, fontSize: 10)),
        ],
      ),
    ),
  ),
);
Widget empty(String text, IconData icon) => Center(
  child: Padding(
    padding: const EdgeInsets.all(25),
    child: Column(
      mainAxisSize: MainAxisSize.min,
      children: [
        Icon(icon, size: 30, color: muted),
        const SizedBox(height: 13),
        Text(
          text,
          textAlign: TextAlign.center,
          style: const TextStyle(color: muted, height: 1.6, fontSize: 12),
        ),
      ],
    ),
  ),
);
Widget flow(
  String number,
  String title,
  String subtitle,
  IconData icon,
) => Padding(
  padding: const EdgeInsets.only(bottom: 17),
  child: Row(
    children: [
      Container(
        width: 36,
        height: 36,
        decoration: BoxDecoration(
          color: paper,
          borderRadius: BorderRadius.circular(8),
        ),
        child: Icon(icon, color: teal, size: 19),
      ),
      const SizedBox(width: 13),
      Expanded(
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.start,
          children: [
            Text(
              title,
              style: const TextStyle(fontWeight: FontWeight.w600, fontSize: 12),
            ),
            const SizedBox(height: 5),
            Text(subtitle, style: const TextStyle(color: muted, fontSize: 10)),
          ],
        ),
      ),
      Text(
        number,
        style: const TextStyle(
          color: muted,
          fontFamily: 'monospace',
          fontSize: 11,
        ),
      ),
    ],
  ),
);

class TrafficPainter extends CustomPainter {
  final List<Map<String, dynamic>> points;
  TrafficPainter(this.points);
  @override
  void paint(Canvas canvas, Size size) {
    final grid = Paint()
      ..color = edge
      ..strokeWidth = 1;
    for (var i = 0; i < 5; i++) {
      final y = i * (size.height - 20) / 4;
      canvas.drawLine(Offset(35, y), Offset(size.width, y), grid);
    }
    final maximum = math.max(
      1,
      points
          .map((point) => (point['successful'] as num).toDouble())
          .reduce(math.max),
    );
    final end = math.max(1, (points.last['second'] as num).toDouble());
    final line = Path();
    for (var index = 0; index < points.length; index++) {
      final x =
          35 +
          (points[index]['second'] as num).toDouble() / end * (size.width - 45);
      final y =
          (size.height - 20) *
          (1 - (points[index]['successful'] as num).toDouble() / maximum);
      if (index == 0) {
        line.moveTo(x, y);
      } else {
        line.lineTo(x, y);
      }
      canvas.drawCircle(Offset(x, y), 3, Paint()..color = teal);
    }
    final fill = Path.from(line)
      ..lineTo(size.width - 10, size.height - 20)
      ..lineTo(35, size.height - 20)
      ..close();
    canvas.drawPath(
      fill,
      Paint()
        ..shader = const LinearGradient(
          begin: Alignment.topCenter,
          end: Alignment.bottomCenter,
          colors: [Color(0x33087F72), Color(0x00087F72)],
        ).createShader(Offset.zero & size),
    );
    canvas.drawPath(
      line,
      Paint()
        ..color = teal
        ..style = PaintingStyle.stroke
        ..strokeWidth = 2.5,
    );
    for (var i = 0; i < 5; i++) {
      final painter = TextPainter(
        text: TextSpan(
          text: '${(maximum * (1 - i / 4)).round()}',
          style: const TextStyle(color: muted, fontSize: 9),
        ),
        textDirection: TextDirection.ltr,
      )..layout();
      painter.paint(canvas, Offset(0, i * (size.height - 20) / 4));
    }
  }

  @override
  bool shouldRepaint(covariant TrafficPainter oldDelegate) => true;
}

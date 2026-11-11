"""A dependency-free SVG precision/recall chart for docs/RESULTS.md (D-043).

One point per profile: recall on x, precision on y. Both axes are zoomed to the
data (stated in the axis labels), because profiles differ by a few points and a
0-1 scale would put all three on top of each other. Colors are the first three
slots of a validated categorical palette, with dark-mode steps; every point is
also labeled directly, so identity never depends on color alone.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from xml.sax.saxutils import escape

WIDTH, HEIGHT = 640, 420
LABEL_CLEARANCE = 40  # px
LEFT, RIGHT, TOP, BOTTOM = 72, 32, 56, 64

# Categorical slots 1-3 (blue, orange, aqua), light and dark steps.
SERIES_LIGHT = ("#2a78d6", "#eb6834", "#1baf7a")
SERIES_DARK = ("#3987e5", "#d95926", "#199e70")


def _series_rules(colors: tuple[str, ...]) -> str:
    return " ".join(f".s{i} {{ fill: {color}; }}" for i, color in enumerate(colors))


STYLE = f"""
  .surface {{ fill: #fcfcfb; }}
  .ink {{ fill: #0b0b0b; }}
  .ink-2 {{ fill: #52514e; }}
  .muted {{ fill: #898781; }}
  .grid {{ stroke: #e1e0d9; stroke-width: 1; }}
  .axis {{ stroke: #c3c2b7; stroke-width: 1; }}
  .ring {{ stroke: #fcfcfb; stroke-width: 2; }}
  {_series_rules(SERIES_LIGHT)}
  @media (prefers-color-scheme: dark) {{
    .surface {{ fill: #1a1a19; }}
    .ink {{ fill: #f0efec; }}
    .ink-2 {{ fill: #c3c2b7; }}
    .grid {{ stroke: #2c2c2a; }}
    .axis {{ stroke: #383835; }}
    .ring {{ stroke: #1a1a19; }}
    {_series_rules(SERIES_DARK)}
  }}
  text {{ font-family: -apple-system, "Segoe UI", Helvetica, Arial, sans-serif; }}
"""


@dataclass(frozen=True)
class Point:
    label: str
    recall: float
    precision: float


def _axis_range(values: list[float], step: float) -> tuple[float, float]:
    """A range on ``step`` boundaries that contains every value, capped at 1."""
    low = math.floor((min(values) - step / 2) / step) * step
    high = min(1.0, math.ceil((max(values) + step / 2) / step) * step)
    return max(0.0, low), high


def _ticks(low: float, high: float, step: float) -> list[float]:
    count = round((high - low) / step)
    return [round(low + i * step, 6) for i in range(count + 1)]


def _label_sides(points, x, y, plot_w) -> list[tuple[str, int]]:
    """Put each label right of its point, or left near the right edge. A label
    whose point is within ``LABEL_CLEARANCE`` px of an earlier point goes on
    the other side of it, so nearly equal profiles stay readable."""
    sides = []
    for i, p in enumerate(points):
        right = x(p.recall) <= LEFT + plot_w * 0.7
        for j in range(i):
            near = math.dist(
                (x(p.recall), y(p.precision)), (x(points[j].recall), y(points[j].precision))
            )
            if near < LABEL_CLEARANCE:
                right = sides[j][0] == "end"
        sides.append(("start", 12) if right else ("end", -12))
    return sides


def precision_recall_svg(points: list[Point], title: str, step: float = 0.05) -> str:
    """Render the chart; at most three points (one per profile)."""
    if not 1 <= len(points) <= len(SERIES_LIGHT):
        raise ValueError(f"Expected 1-{len(SERIES_LIGHT)} points, got {len(points)}")
    x_low, x_high = _axis_range([p.recall for p in points], step)
    y_low, y_high = _axis_range([p.precision for p in points], step / 5)
    plot_w, plot_h = WIDTH - LEFT - RIGHT, HEIGHT - TOP - BOTTOM

    def x(value: float) -> float:
        return LEFT + (value - x_low) / (x_high - x_low) * plot_w

    def y(value: float) -> float:
        return TOP + plot_h - (value - y_low) / (y_high - y_low) * plot_h

    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {WIDTH} {HEIGHT}" '
        f'width="{WIDTH}" height="{HEIGHT}" role="img" aria-labelledby="t">',
        f'<title id="t">{escape(title)}</title>',
        f"<style>{STYLE}</style>",
        f'<rect class="surface" width="{WIDTH}" height="{HEIGHT}" rx="8"/>',
        f'<text class="ink" x="{LEFT}" y="28" font-size="16" font-weight="600">'
        f"{escape(title)}</text>",
    ]
    # Legend, one row under the title.
    legend_x = LEFT
    for i, p in enumerate(points):
        out.append(f'<circle class="s{i}" cx="{legend_x + 5}" cy="44" r="5"/>')
        out.append(
            f'<text class="ink-2" x="{legend_x + 14}" y="48" font-size="12">'
            f"{escape(p.label)}</text>"
        )
        legend_x += 24 + 7 * len(p.label) + 16

    for tick in _ticks(x_low, x_high, step):
        out.append(
            f'<line class="grid" x1="{x(tick):.1f}" x2="{x(tick):.1f}" '
            f'y1="{TOP}" y2="{TOP + plot_h}"/>'
        )
        out.append(
            f'<text class="muted" x="{x(tick):.1f}" y="{TOP + plot_h + 18}" '
            f'font-size="11" text-anchor="middle">{tick:.2f}</text>'
        )
    for tick in _ticks(y_low, y_high, step / 5):
        out.append(
            f'<line class="grid" x1="{LEFT}" x2="{LEFT + plot_w}" '
            f'y1="{y(tick):.1f}" y2="{y(tick):.1f}"/>'
        )
        out.append(
            f'<text class="muted" x="{LEFT - 8}" y="{y(tick) + 4:.1f}" '
            f'font-size="11" text-anchor="end">{tick:.2f}</text>'
        )
    out.append(
        f'<line class="axis" x1="{LEFT}" x2="{LEFT + plot_w}" '
        f'y1="{TOP + plot_h}" y2="{TOP + plot_h}"/>'
    )
    out.append(f'<line class="axis" x1="{LEFT}" x2="{LEFT}" y1="{TOP}" y2="{TOP + plot_h}"/>')
    out.append(
        f'<text class="ink-2" x="{LEFT + plot_w / 2}" y="{HEIGHT - 20}" font-size="12" '
        f'text-anchor="middle">Pairwise recall (axis from {x_low:.2f})</text>'
    )
    out.append(
        f'<text class="ink-2" font-size="12" text-anchor="middle" '
        f'transform="translate(20 {TOP + plot_h / 2}) rotate(-90)">'
        f"Pairwise precision (axis from {y_low:.2f})</text>"
    )

    for i, (p, (anchor, dx)) in enumerate(
        zip(points, _label_sides(points, x, y, plot_w), strict=True)
    ):
        px, py = x(p.recall), y(p.precision)
        out.append(
            f'<circle class="s{i} ring" cx="{px:.1f}" cy="{py:.1f}" r="6">'
            f"<title>{escape(p.label)}: precision {p.precision:.4f}, "
            f"recall {p.recall:.4f}</title></circle>"
        )
        out.append(
            f'<text class="ink" x="{px + dx:.1f}" y="{py - 4:.1f}" font-size="12" '
            f'text-anchor="{anchor}">{escape(p.label)}</text>'
        )
        out.append(
            f'<text class="ink-2" x="{px + dx:.1f}" y="{py + 11:.1f}" font-size="11" '
            f'text-anchor="{anchor}">P {p.precision:.3f} · R {p.recall:.3f}</text>'
        )
    out.append("</svg>")
    return "\n".join(out) + "\n"

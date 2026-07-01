"""
visualize.py
------------
Generates a pipeline DAG diagram and run-metrics figures,
simulating the visual monitoring dashboard of a cloud pipeline
orchestration tool.
"""

from typing import Dict, List
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

from pipeline_core import Pipeline, StageResult

plt.rcParams.update({
    "figure.dpi": 150,
    "savefig.dpi": 300,
    "font.size": 10,
})

STAGE_COLORS = {
    "extract": "#1f4e8c",
    "transform": "#2f7a4f",
    "load": "#b8433d",
    "quality_check": "#7a5195",
}


def _layered_positions(pipeline: Pipeline) -> Dict[str, tuple]:
    """Assign each stage an (x, y) position based on its DAG depth (layer)."""
    order = pipeline.execution_order()
    depth = {name: 0 for name in pipeline.stages}

    for name in order:
        stage = pipeline.stages[name]
        if stage.depends_on:
            depth[name] = max(depth[dep] for dep in stage.depends_on) + 1

    layers: Dict[int, List[str]] = {}
    for name, d in depth.items():
        layers.setdefault(d, []).append(name)

    positions = {}
    for d, names in layers.items():
        names.sort()
        for i, name in enumerate(names):
            y = -(i - (len(names) - 1) / 2)
            positions[name] = (d * 2.6, y * 1.4)

    return positions


def plot_pipeline_dag(pipeline: Pipeline, save_path: str):
    positions = _layered_positions(pipeline)

    fig, ax = plt.subplots(figsize=(max(8, 2.6 * (max(p[0] for p in positions.values()) / 2.6 + 1)), 6))

    for stage_name, stage in pipeline.stages.items():
        for dep in stage.depends_on:
            x1, y1 = positions[dep]
            x2, y2 = positions[stage_name]
            arrow = FancyArrowPatch(
                (x1 + 0.55, y1), (x2 - 0.55, y2),
                arrowstyle="-|>", mutation_scale=14,
                color="#888888", linewidth=1.2, zorder=1,
            )
            ax.add_patch(arrow)

    for stage_name, stage in pipeline.stages.items():
        x, y = positions[stage_name]
        color = STAGE_COLORS.get(stage.stage_type, "#555555")
        box = FancyBboxPatch(
            (x - 0.55, y - 0.28), 1.1, 0.56,
            boxstyle="round,pad=0.02,rounding_size=0.08",
            linewidth=1.4, edgecolor=color, facecolor=color, alpha=0.18, zorder=2,
        )
        ax.add_patch(box)
        ax.text(x, y, stage_name, ha="center", va="center", fontsize=9.5,
                 fontweight="bold", color=color, zorder=3)

    legend_handles = [mpatches.Patch(color=c, label=t.replace("_", " ").title())
                       for t, c in STAGE_COLORS.items()]
    ax.legend(handles=legend_handles, loc="upper center", bbox_to_anchor=(0.5, -0.02),
               ncol=4, frameon=False, fontsize=9)

    ax.set_xlim(min(p[0] for p in positions.values()) - 1, max(p[0] for p in positions.values()) + 1)
    ax.set_ylim(min(p[1] for p in positions.values()) - 1, max(p[1] for p in positions.values()) + 1)
    ax.set_title(f"Pipeline DAG — {pipeline.name}", fontsize=13, fontweight="bold")
    ax.axis("off")
    fig.tight_layout()
    fig.savefig(save_path, bbox_inches="tight")
    plt.close(fig)


def plot_run_durations(run_log: List[StageResult], save_path: str):
    names = [r.stage_name for r in run_log]
    durations = [r.duration_seconds for r in run_log]
    colors = ["#2f7a4f" if r.status == "success" else "#b8433d" if r.status == "failed" else "#999999"
              for r in run_log]

    fig, ax = plt.subplots(figsize=(8, max(3, 0.5 * len(names))))
    bars = ax.barh(names, durations, color=colors)
    ax.invert_yaxis()
    ax.set_xlabel("Duration (seconds)")
    ax.set_title("Pipeline Stage Execution Time")
    for bar, r in zip(bars, run_log):
        ax.annotate(f"{r.status}", (bar.get_width(), bar.get_y() + bar.get_height() / 2),
                    textcoords="offset points", xytext=(5, 0), va="center", fontsize=8)
    fig.tight_layout()
    fig.savefig(save_path)
    plt.close(fig)


def plot_row_throughput(run_log: List[StageResult], save_path: str):
    filtered = [r for r in run_log if r.rows_processed is not None]
    if not filtered:
        return
    names = [r.stage_name for r in filtered]
    rows = [r.rows_processed for r in filtered]

    fig, ax = plt.subplots(figsize=(8, max(3, 0.5 * len(names))))
    ax.barh(names, rows, color="#1f4e8c")
    ax.invert_yaxis()
    ax.set_xlabel("Rows Processed")
    ax.set_title("Pipeline Stage Row Throughput")
    fig.tight_layout()
    fig.savefig(save_path)
    plt.close(fig)

"""
pipeline_core.py
-----------------
Core framework for the Cloud Data Pipeline Builder: defines
pipeline stages as a directed acyclic graph (DAG), resolves
execution order via topological sort, and runs stages while
tracking status, timing, and errors — simulating the orchestration
core of a cloud pipeline tool (e.g., Airflow / Cloud Composer /
Step Functions equivalent, implemented standalone).
"""

import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Optional, Any


class PipelineError(Exception):
    """Raised when the pipeline graph is invalid (e.g., contains a cycle)."""
    pass


@dataclass
class StageResult:
    stage_name: str
    status: str  # "success" | "failed" | "skipped"
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_seconds: float = 0.0
    rows_processed: Optional[int] = None
    error: Optional[str] = None


class Stage:
    """
    A single unit of work in a pipeline (extract, transform, or load).

    Parameters
    ----------
    name : str
        Unique stage identifier.
    func : Callable
        Function executed for this stage. Receives a shared `context`
        dict (holding outputs of upstream stages) and must return a
        value to be stored in the context under `name`.
    depends_on : list[str]
        Names of upstream stages that must complete before this one runs.
    stage_type : str
        One of "extract", "transform", "load" — used for DAG visualization
        and reporting only.
    """

    def __init__(self, name: str, func: Callable, depends_on: Optional[List[str]] = None,
                 stage_type: str = "transform"):
        self.name = name
        self.func = func
        self.depends_on = depends_on or []
        self.stage_type = stage_type


class Pipeline:
    """
    A configurable, DAG-based data pipeline. Stages are added via
    `add_stage`, dependencies are resolved automatically, and the
    pipeline executes stages in valid topological order.
    """

    def __init__(self, name: str):
        self.name = name
        self.stages: Dict[str, Stage] = {}
        self.context: Dict[str, Any] = {}
        self.run_log: List[StageResult] = []

    def add_stage(self, stage: Stage) -> "Pipeline":
        if stage.name in self.stages:
            raise PipelineError(f"Duplicate stage name: '{stage.name}'")
        self.stages[stage.name] = stage
        return self

    # ---- DAG resolution ----

    def _topological_order(self) -> List[str]:
        """Kahn's algorithm for topological sort with cycle detection."""
        in_degree = {name: 0 for name in self.stages}
        adjacency: Dict[str, List[str]] = {name: [] for name in self.stages}

        for stage in self.stages.values():
            for dep in stage.depends_on:
                if dep not in self.stages:
                    raise PipelineError(
                        f"Stage '{stage.name}' depends on unknown stage '{dep}'"
                    )
                adjacency[dep].append(stage.name)
                in_degree[stage.name] += 1

        queue = [name for name, deg in in_degree.items() if deg == 0]
        ordered = []

        while queue:
            queue.sort()  # deterministic ordering among ready stages
            current = queue.pop(0)
            ordered.append(current)
            for downstream in adjacency[current]:
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    queue.append(downstream)

        if len(ordered) != len(self.stages):
            raise PipelineError("Cycle detected in pipeline DAG — cannot resolve execution order.")

        return ordered

    def execution_order(self) -> List[str]:
        return self._topological_order()

    # ---- Execution ----

    def run(self, stop_on_failure: bool = True) -> List[StageResult]:
        self.run_log = []
        self.context = {}
        failed_upstream = set()

        for stage_name in self._topological_order():
            stage = self.stages[stage_name]

            if any(dep in failed_upstream for dep in stage.depends_on):
                result = StageResult(stage_name=stage_name, status="skipped")
                self.run_log.append(result)
                failed_upstream.add(stage_name)
                continue

            started = datetime.now()
            t0 = time.perf_counter()
            try:
                output = stage.func(self.context)
                self.context[stage_name] = output
                duration = time.perf_counter() - t0
                rows = _infer_row_count(output)
                result = StageResult(
                    stage_name=stage_name,
                    status="success",
                    started_at=started.isoformat(timespec="seconds"),
                    finished_at=datetime.now().isoformat(timespec="seconds"),
                    duration_seconds=round(duration, 4),
                    rows_processed=rows,
                )
            except Exception as exc:  # noqa: BLE001
                duration = time.perf_counter() - t0
                result = StageResult(
                    stage_name=stage_name,
                    status="failed",
                    started_at=started.isoformat(timespec="seconds"),
                    finished_at=datetime.now().isoformat(timespec="seconds"),
                    duration_seconds=round(duration, 4),
                    error=f"{exc.__class__.__name__}: {exc}",
                )
                failed_upstream.add(stage_name)
                if stop_on_failure:
                    self.run_log.append(result)
                    print(f"[Pipeline:{self.name}] Stage '{stage_name}' failed:")
                    print(traceback.format_exc())
                    break

            self.run_log.append(result)

        return self.run_log

    def summary(self) -> Dict[str, Any]:
        statuses = [r.status for r in self.run_log]
        return {
            "pipeline": self.name,
            "total_stages": len(self.run_log),
            "succeeded": statuses.count("success"),
            "failed": statuses.count("failed"),
            "skipped": statuses.count("skipped"),
            "total_duration_seconds": round(sum(r.duration_seconds for r in self.run_log), 4),
        }


def _infer_row_count(output: Any) -> Optional[int]:
    """Best-effort row count for reporting, without hard-coding pandas as a requirement."""
    try:
        return len(output)
    except TypeError:
        return None

"""Virtual-time workload schedules."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, replace
from enum import StrEnum

from ephemeraldb.loadgen.models import BenchmarkPhase


class ScheduleShape(StrEnum):
    CONSTANT = "CONSTANT"
    RAMP = "RAMP"
    SPIKE = "SPIKE"
    STEP = "STEP"
    COOLDOWN = "COOLDOWN"


@dataclass(frozen=True, slots=True)
class WorkloadSegment:
    name: str
    phase: BenchmarkPhase
    duration_seconds: int
    start_rate: int
    end_rate: int
    shape: ScheduleShape = ScheduleShape.CONSTANT

    def __post_init__(self) -> None:
        if not self.name.strip():
            msg = "segment name must not be empty"
            raise ValueError(msg)
        _require_at_least("duration_seconds", self.duration_seconds, 1)
        _require_at_least("start_rate", self.start_rate, 0)
        _require_at_least("end_rate", self.end_rate, 0)

    @property
    def operation_count(self) -> int:
        average_rate = (self.start_rate + self.end_rate) / 2
        return round(self.duration_seconds * average_rate)


@dataclass(frozen=True, slots=True)
class ScheduledOperationSlot:
    scheduled_offset_ms: int
    benchmark_phase: BenchmarkPhase


@dataclass(frozen=True, slots=True)
class WorkloadSchedule:
    name: str
    segments: tuple[WorkloadSegment, ...]

    def __post_init__(self) -> None:
        if not self.name.strip():
            msg = "schedule name must not be empty"
            raise ValueError(msg)
        if not self.segments:
            msg = "schedule must include at least one segment"
            raise ValueError(msg)

    @property
    def duration_seconds(self) -> int:
        return sum(segment.duration_seconds for segment in self.segments)

    @property
    def operation_count(self) -> int:
        return sum(segment.operation_count for segment in self.segments)

    @property
    def peak_rate(self) -> int:
        return max(max(segment.start_rate, segment.end_rate) for segment in self.segments)

    def with_target_rate(self, target_rate: int) -> WorkloadSchedule:
        _require_at_least("target_rate", target_rate, 1)
        peak = self.peak_rate
        if peak == 0:
            return self
        scale = target_rate / peak
        return replace(
            self,
            segments=tuple(
                replace(
                    segment,
                    start_rate=max(1, round(segment.start_rate * scale)),
                    end_rate=max(1, round(segment.end_rate * scale)),
                )
                for segment in self.segments
            ),
        )

    def with_measurement_duration(self, duration_seconds: int) -> WorkloadSchedule:
        _require_at_least("duration_seconds", duration_seconds, 1)
        return replace(
            self,
            segments=tuple(
                replace(segment, duration_seconds=duration_seconds)
                if segment.phase == BenchmarkPhase.MEASUREMENT
                else segment
                for segment in self.segments
            ),
        )


def iter_schedule_slots(schedule: WorkloadSchedule) -> Iterator[ScheduledOperationSlot]:
    segment_start_ms = 0
    for segment in schedule.segments:
        count = segment.operation_count
        duration_ms = segment.duration_seconds * 1000
        if count > 0:
            for operation_index in range(count):
                offset_within_segment = int((operation_index * duration_ms) / count)
                yield ScheduledOperationSlot(
                    scheduled_offset_ms=segment_start_ms + offset_within_segment,
                    benchmark_phase=segment.phase,
                )
        segment_start_ms += duration_ms


def get_schedule_preset(name: str) -> WorkloadSchedule:
    normalized_name = name.strip().lower().replace("_", "-")
    if normalized_name == "small-dev":
        return small_dev_schedule()
    if normalized_name == "ramp-1000":
        return ramp_1000_schedule()
    msg = f"unknown workload schedule: {name}"
    raise ValueError(msg)


def schedule_preset_names() -> tuple[str, ...]:
    return ("small-dev", "ramp-1000")


def constant_schedule(
    *,
    name: str,
    duration_seconds: int,
    rate: int,
    phase: BenchmarkPhase = BenchmarkPhase.MEASUREMENT,
) -> WorkloadSchedule:
    return WorkloadSchedule(
        name=name,
        segments=(
            WorkloadSegment(
                name="constant",
                phase=phase,
                duration_seconds=duration_seconds,
                start_rate=rate,
                end_rate=rate,
                shape=ScheduleShape.CONSTANT,
            ),
        ),
    )


def small_dev_schedule() -> WorkloadSchedule:
    return WorkloadSchedule(
        name="SMALL_DEV",
        segments=(
            WorkloadSegment("warmup", BenchmarkPhase.WARMUP, 10, 10, 10),
            WorkloadSegment("measurement", BenchmarkPhase.MEASUREMENT, 30, 50, 50),
            WorkloadSegment(
                "cooldown",
                BenchmarkPhase.COOLDOWN,
                10,
                10,
                10,
                ScheduleShape.COOLDOWN,
            ),
        ),
    )


def ramp_1000_schedule() -> WorkloadSchedule:
    return WorkloadSchedule(
        name="RAMP_1000",
        segments=(
            WorkloadSegment("warmup", BenchmarkPhase.WARMUP, 30, 50, 50),
            WorkloadSegment(
                "ramp-50-250",
                BenchmarkPhase.MEASUREMENT,
                60,
                50,
                250,
                ScheduleShape.RAMP,
            ),
            WorkloadSegment(
                "ramp-250-500",
                BenchmarkPhase.MEASUREMENT,
                60,
                250,
                500,
                ScheduleShape.RAMP,
            ),
            WorkloadSegment(
                "ramp-500-1000",
                BenchmarkPhase.MEASUREMENT,
                60,
                500,
                1000,
                ScheduleShape.RAMP,
            ),
            WorkloadSegment(
                "cooldown",
                BenchmarkPhase.COOLDOWN,
                30,
                50,
                50,
                ScheduleShape.COOLDOWN,
            ),
        ),
    )


def spike_schedule(
    *,
    name: str,
    baseline_rate: int,
    spike_rate: int,
    baseline_seconds: int,
    spike_seconds: int,
) -> WorkloadSchedule:
    return WorkloadSchedule(
        name=name,
        segments=(
            WorkloadSegment(
                "baseline-before",
                BenchmarkPhase.MEASUREMENT,
                baseline_seconds,
                baseline_rate,
                baseline_rate,
                ScheduleShape.CONSTANT,
            ),
            WorkloadSegment(
                "spike",
                BenchmarkPhase.MEASUREMENT,
                spike_seconds,
                spike_rate,
                spike_rate,
                ScheduleShape.SPIKE,
            ),
            WorkloadSegment(
                "baseline-after",
                BenchmarkPhase.COOLDOWN,
                baseline_seconds,
                baseline_rate,
                baseline_rate,
                ScheduleShape.COOLDOWN,
            ),
        ),
    )


def step_schedule(
    *,
    name: str,
    step_rates: tuple[int, ...],
    seconds_per_step: int,
) -> WorkloadSchedule:
    if not step_rates:
        msg = "step_rates must not be empty"
        raise ValueError(msg)
    return WorkloadSchedule(
        name=name,
        segments=tuple(
            WorkloadSegment(
                f"step-{index + 1}",
                BenchmarkPhase.MEASUREMENT,
                seconds_per_step,
                rate,
                rate,
                ScheduleShape.STEP,
            )
            for index, rate in enumerate(step_rates)
        ),
    )


def _require_at_least(name: str, value: int, minimum: int) -> None:
    if type(value) is not int:
        msg = f"{name} must be an integer"
        raise TypeError(msg)
    if value < minimum:
        msg = f"{name} must be >= {minimum}"
        raise ValueError(msg)

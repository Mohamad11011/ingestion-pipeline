from __future__ import annotations

from datetime import date, timedelta
from typing import Literal

PartitionSize = Literal["monthly", "weekly"]


class DatePartition:
    def __init__(self, start: date, end: date) -> None:
        if end <= start:
            raise ValueError("partition end must be after start")
        self.start = start
        self.end = end

    @property
    def partition_date(self) -> date:
        return self.start

    def site_from(self) -> str:
        return _site_date(self.start)

    def site_to(self) -> str:
        return _site_date(self.end - timedelta(days=1))


def generate_partitions(
    start_date: date,
    end_date: date,
    size: PartitionSize = "monthly",
) -> list[DatePartition]:
    if end_date <= start_date:
        return []

    partitions: list[DatePartition] = []
    cursor = start_date
    while cursor < end_date:
        nxt = _advance(cursor, size)
        partitions.append(DatePartition(cursor, min(nxt, end_date)))
        cursor = nxt
    return partitions


def _advance(value: date, size: PartitionSize) -> date:
    if size == "weekly":
        return value + timedelta(days=7)
    if size == "monthly":
        if value.month == 12:
            return date(value.year + 1, 1, 1)
        return date(value.year, value.month + 1, 1)
    raise ValueError(f"unsupported partition size: {size}")


def _site_date(value: date) -> str:
    return f"{value.day}/{value.month}/{value.year}"

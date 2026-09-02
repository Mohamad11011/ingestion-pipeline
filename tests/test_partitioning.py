from datetime import date

from partitioning.dates import generate_partitions


def test_monthly_partitions_are_half_open_and_exclude_end_date() -> None:
    partitions = generate_partitions(
        date(2024, 1, 1),
        date(2024, 4, 1),
        size="monthly",
    )

    assert [(p.start, p.end) for p in partitions] == [
        (date(2024, 1, 1), date(2024, 2, 1)),
        (date(2024, 2, 1), date(2024, 3, 1)),
        (date(2024, 3, 1), date(2024, 4, 1)),
    ]
    assert [p.partition_date for p in partitions] == [
        date(2024, 1, 1),
        date(2024, 2, 1),
        date(2024, 3, 1),
    ]


def test_site_query_dates_use_inclusive_to() -> None:
    partition = generate_partitions(
        date(2024, 1, 1),
        date(2024, 2, 1),
        size="monthly",
    )[0]

    assert partition.site_from() == "1/1/2024"
    assert partition.site_to() == "31/1/2024"


def test_weekly_partitions_step_seven_days() -> None:
    partitions = generate_partitions(
        date(2024, 1, 1),
        date(2024, 1, 15),
        size="weekly",
    )

    assert [(p.start, p.end) for p in partitions] == [
        (date(2024, 1, 1), date(2024, 1, 8)),
        (date(2024, 1, 8), date(2024, 1, 15)),
    ]

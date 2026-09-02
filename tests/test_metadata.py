from models.metadata import LandingMetadata


def test_record_identity_is_body_plus_identifier() -> None:
    record = LandingMetadata(
        identifier="ADJ-00039955",
        title="A Chef v A Health Service Provider",
        description="A Chef v A Health Service Provider",
        date="2024-02-01",
        document_url="https://www.workplacerelations.ie/en/cases/2024/february/adj-00039955.html",
        partition_date="2024-01-01",
        body="Workplace Relations Commission",
        file_path="Workplace Relations Commission/ADJ-00039955.html",
        file_hash="abc",
    )

    assert record.identity == ("Workplace Relations Commission", "ADJ-00039955")

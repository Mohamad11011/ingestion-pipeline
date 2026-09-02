from __future__ import annotations

from collections.abc import Iterator
from datetime import date

from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection

from config.settings import Settings, get_settings

_UNIQUE_INDEX = "uniq_body_identifier"
_PARTITION_INDEX = "partition_date"


class MongoRepository:
    """Landing and transformed metadata collections keyed on body + identifier."""

    def __init__(
        self,
        settings: Settings | None = None,
        client: MongoClient | None = None,
    ) -> None:
        self._settings = settings or get_settings()
        self._client = client or MongoClient(self._settings.mongo_uri)
        database = self._client[self._settings.mongo_database]
        self.landing: Collection = database[self._settings.mongo_landing_collection]
        self.transformed: Collection = database[self._settings.mongo_transformed_collection]

    def ensure_indexes(self) -> None:
        for collection in (self.landing, self.transformed):
            collection.create_index(
                [("body", ASCENDING), ("identifier", ASCENDING)],
                unique=True,
                name=_UNIQUE_INDEX,
            )
            collection.create_index([("partition_date", ASCENDING)], name=_PARTITION_INDEX)

    def find_landing(self, body: str, identifier: str) -> dict | None:
        return self.landing.find_one(
            {"body": body, "identifier": identifier},
            projection={"_id": False},
        )

    def upsert_landing(self, record: dict) -> bool:
        """Upsert on identity. Returns True when a new document was inserted."""
        return self._upsert(self.landing, record)

    def upsert_transformed(self, record: dict) -> bool:
        return self._upsert(self.transformed, record)

    def iter_landing_by_partition_range(
        self,
        start_date: date | str,
        end_date: date | str,
        batch_size: int = 500,
    ) -> Iterator[dict]:
        """Stream landing records where start <= partition_date < end."""
        cursor = (
            self.landing.find(
                {
                    "partition_date": {
                        "$gte": _as_iso(start_date),
                        "$lt": _as_iso(end_date),
                    }
                },
                projection={"_id": False},
            )
            .sort([("partition_date", ASCENDING), ("identifier", ASCENDING)])
            .batch_size(batch_size)
        )
        yield from cursor

    def close(self) -> None:
        self._client.close()

    @staticmethod
    def _upsert(collection: Collection, record: dict) -> bool:
        identity = {"body": record["body"], "identifier": record["identifier"]}
        payload = {key: value for key, value in record.items() if key not in identity}
        result = collection.update_one(
            identity,
            {"$set": payload, "$setOnInsert": identity},
            upsert=True,
        )
        return result.upserted_id is not None


def _as_iso(value: date | str) -> str:
    return value.isoformat() if isinstance(value, date) else value

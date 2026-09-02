# Workplace Relations scraper

Ingestion and transformation pipeline for the Workplace Relations decisions database. Company test requirements are in `Workplace_Relations_Coding_Test_Requirements_Plan.md`.

## Setup

```powershell
copy .env.example .env
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
docker compose --env-file .env up -d
pytest
```

Infrastructure: MongoDB `localhost:27017`, MinIO S3 API `localhost:9000`, console `localhost:9001`. `minio-init` creates buckets `landing` and `transformed`.

Do not commit `.env`.

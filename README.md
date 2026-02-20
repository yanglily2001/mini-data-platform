# Mini Data Platform
A full-stack data platform for ingesting, validating, storing, aggregating, and visualizing structured time-series data (e.g., weather measurements).

The system supports:

* CSV ingestion with validation

* Staging + production tables

* Idempotent upsert behavior

* Data quality reporting

* Aggregate metrics (daily + summary)

* Interactive frontend visualization

The backend is built with Django and PostgreSQL.
The frontend is built with Vite and React.
Everything runs in Docker for reproducibility.

## Overview

This project demonstrates:

* Data ingestion pipelines

* Validation and quality monitoring

* Idempotent upsert logic

* REST API design

* Database indexing for performance

* Frontend data visualization

* Automated testing

It is designed as a clean, extensible foundation for structured data processing systems.

## Project Structure and Architecture

.
├── backend/                # Django backend (API, models, validation logic)
│   ├── metrics/            # Core app
│   ├── config/             # Django settings
│   └── manage.py
│
├── frontend/               # Vite React frontend
│   └── src/
│
├── data/                   # Sample CSV data
├── docker-compose.yml
├── .env.example
├── Makefile
└── README.md

## Technology Stack

Backend

* Python 3.12

* Django

* Django REST Framework

* PostgreSQL

* Docker

Frontend

* Vite

* React

* Recharts (for charts)

Tooling

* Docker Compose

* Make (optional shortcuts)

* dotenv (.env configuration)

* curl (for API testing)
  
| Layer       | Technology |
|-------------|------------|
| Backend     | Django + DRF |
| Frontend    | Vite + React |
| Database    | PostgreSQL |
| Infra       | Docker |

## Quickstart
Clone the repository:
git clone <your-repo-url>
cd mini-data-platform

Create environment file:
cp .env.example .env

Build and start services:
docker compose up --build

Services:

* Frontend → http://localhost:5173

* Backend API → http://localhost:8000

* Admin → http://localhost:8000/admin

Apply migrations:

docker exec -it data_platform_backend python manage.py migrate

Generate sample data:

docker exec -it data_platform_backend \
python manage.py generate_sample_csv --rows 200 --out data/sample.csv

Import sample data:

curl -X POST http://localhost:8000/api/import/ \
  -F "file=@data/sample.csv"
  
## API Reference (Endpoints)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health/` | Health check, returns { "status": "ok" } |
| POST | `/api/import/` | Upload CSV, returns row information, valid rows upserted into production table, idempotent import |
| GET | `/api/stations/` | List station IDs |
| GET | `/api/metrics/daily/` | Daily metrics, returns station ID and count and data |
| GET | `/api/metrics/summary/` | Aggregated metrics, returns station ID, number of rows, total precipitation, average temperature data |
| GET | `/api/quality/` | Data quality report |

## Data Model
Staging Measurement

Purpose: raw ingestion table.

Fields:

* id (PK)

* date

* station_id

* temp_c

* precip_mm

All rows from CSV are inserted here, including invalid rows.

Measurement

Production table.

Fields:

* id (PK)

* date

* station_id

* temp_c

* precip_mm

Constraints:

* Unique (station_id, date)

Indexes:

* Index on station_id

* Index on date

* Composite index on (station_id, date)

Upsert logic:

* Uses bulk_create(update_conflicts=True)

* Ensures idempotent import

## Data Quality

Validation during import:

* Valid date format

* Required station_id

* Temperature range validation

* Precipitation range validation

Quality endpoint reports:

* Null counts per column

* Null rates

* Duplicate detection (by station_id + date)

* Min / max values

* Out-of-range counts

This allows post-ingestion data health analysis.

## Testing
Run inside container:
docker exec -it data_platform_backend \
python manage.py test metrics -v 2

Tests verify:

* CSV validation logic

* Row counts (valid/invalid)

* Idempotent upsert behavior

* Summary endpoint aggregate correctness

* Missing file handling

* Quality endpoint structure

## Development Notes

* Staging table preserves raw ingestion data.

* Production table enforces uniqueness.

* Upsert strategy prevents duplicates.

* Indexes improve filtering by station and date.

* Docker ensures consistent environment.

* Tests run against a dedicated test database.

## Trade-offs

* Bulk upsert chosen over COPY for simplicity.

* No authentication implemented.

* No pagination on endpoints.

* Frontend assumes moderate dataset size.

* No async ingestion pipeline.

## ChatGPT Usage

ChatGPT was used to:

* Scaffold initial project structure

* Debug Django migration and test issues

* Refine bulk upsert implementation

* Improve validation logic

* Generate example test cases

* Refine API response design

* Improve documentation structure and clarity


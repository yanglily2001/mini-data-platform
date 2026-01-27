A full-stack data platform built with Django, Postgres, and Vite for ingesting, processing, and visualizing structured data.

Backend: Django, Django REST Framework

Frontend: Vite, React (or Vue/Svelte/etc.)

Database: PostgreSQL

Infrastructure: Docker, Docker Compose

Tooling: Make, dotenv

**Structure**
.
├── backend/        # Django backend (API, business logic)

├── frontend/       # Vite frontend (UI)

├── data/           # Sample data, fixtures, or datasets

├── infra/          # Infrastructure-related files (scripts, configs)

├── docker-compose.yml

├── .env.example

├── Makefile

├── README.md

└── .gitignore


**Quickstart**
# Clone the repo
git clone <repo-url>
cd data-platform

# Create env file
cp .env.example .env

# Build and start services
docker compose up --build
Then open:

Frontend → http://localhost:5173
Backend API → http://localhost:8000
Admin (if enabled) → http://localhost:8000/admin

**Docker Commands**
# Start services
docker compose up

# Stop services
docker compose down

# Rebuild containers
docker compose up --build

# Reset database
docker compose down -v

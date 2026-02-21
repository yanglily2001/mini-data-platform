# ---- Makefile ----

COMPOSE = docker compose
BACKEND = backend

.PHONY: help up down logs migrate createsuperuser load_sample test

help:
	@echo "Available commands:"
	@echo "  make up              - Start services"
	@echo "  make down            - Stop services"
	@echo "  make logs            - View logs"
	@echo "  make migrate         - Run migrations"
	@echo "  make createsuperuser - Create Django superuser"
	@echo "  make load_sample     - Generate + import sample data"
	@echo "  make test            - Run backend tests"

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

migrate:
	$(COMPOSE) exec -T $(BACKEND) python manage.py migrate

createsuperuser:
	$(COMPOSE) exec -T $(BACKEND) python manage.py createsuperuser

load_sample:
	$(COMPOSE) exec -T $(BACKEND) python manage.py generate_sample_csv --rows 200 --out data/sample.csv
	$(COMPOSE) exec -T $(BACKEND) bash -lc 'curl -s -X POST http://localhost:8000/api/import/ -F "file=@data/sample.csv" | python -m json.tool'

test:
	$(COMPOSE) exec -T $(BACKEND) python manage.py test metrics -v 2

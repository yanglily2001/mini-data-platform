import csv
import os
import random
from datetime import date, timedelta
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Generate a sample CSV file with columns: date,station_id,temp_c,precip_mm"

    def add_arguments(self, parser):
        parser.add_argument(
            "--rows",
            type=int,
            default=100,
            help="Number of rows to generate (default: 100)",
        )
        parser.add_argument(
            "--stations",
            type=str,
            default="STATION_A,STATION_B,STATION_C",
            help="Comma-separated station IDs (default: STATION_A,STATION_B,STATION_C)",
        )
        parser.add_argument(
            "--start-date",
            type=str,
            default=None,
            help="Start date YYYY-MM-DD (default: today - rows days)",
        )
        parser.add_argument(
            "--out",
            type=str,
            default=None,
            help="Output path relative to BASE_DIR, e.g. data/sample.csv (default: data/sample.csv)",
        )
        parser.add_argument(
            "--seed",
            type=int,
            default=42,
            help="Random seed for reproducible output (default: 42)",
        )

    def handle(self, *args, **options):
        rows = options["rows"]
        if rows <= 0:
            raise CommandError("--rows must be > 0")

        stations = [s.strip() for s in options["stations"].split(",") if s.strip()]
        if not stations:
            raise CommandError("--stations must include at least one station id")

        random.seed(options["seed"])

        # Resolve output path safely inside the project
        base_dir = Path(settings.BASE_DIR)  # e.g. /app inside container
        out_rel = options["out"] or "data/sample.csv"
        out_path = (base_dir / out_rel).resolve()

        # Prevent writing outside BASE_DIR (nice safety guard)
        if base_dir not in out_path.parents and out_path != base_dir:
            raise CommandError("Output path must be inside BASE_DIR")

        out_path.parent.mkdir(parents=True, exist_ok=True)

        # Determine start date
        if options["start_date"]:
            try:
                y, m, d = map(int, options["start_date"].split("-"))
                start = date(y, m, d)
            except Exception:
                raise CommandError("--start-date must be YYYY-MM-DD")
        else:
            start = date.today() - timedelta(days=rows)

        # Generate mostly unique (station_id, date) keys (good for your UPSERT + unique constraint)
        # We'll cycle dates forward; station chosen randomly.
        # If rows > number of days range, duplicates can occur; that's OK for testing too.
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "station_id", "temp_c", "precip_mm"])

            for i in range(rows):
                dt = start + timedelta(days=i)
                station_id = random.choice(stations)

                # Valid ranges for your validation rules ✅
                # temp_c: -60..60, precip_mm: 0..500
                temp_c = round(random.uniform(-10, 35), 1)
                precip_mm = round(max(0.0, random.gauss(2.0, 4.0)), 1)
                precip_mm = min(precip_mm, 500.0)

                writer.writerow([dt.isoformat(), station_id, temp_c, precip_mm])

        self.stdout.write(self.style.SUCCESS(f"✅ Wrote {rows} rows to {out_path}"))
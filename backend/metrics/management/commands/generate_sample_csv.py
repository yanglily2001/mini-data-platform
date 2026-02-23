import csv
import random
from datetime import date, timedelta
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Generate a sample CSV file with columns: date,station_id,temp_c,precip_mm (with optional invalid rows)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--rows",
            type=int,
            default=100,
            help="Number of rows to generate (default: 100)",
        )

        # ✅ stations as a real list: --stations A B C
        parser.add_argument(
            "--stations",
            nargs="+",
            default=["STATION_A", "STATION_B", "STATION_C"],
            help="Space-separated station IDs (default: STATION_A STATION_B STATION_C)",
        )

        # ✅ days window
        parser.add_argument(
            "--days",
            type=int,
            default=60,
            help="Date range window in days (default: 60)",
        )

        # ✅ how many invalid rows to inject
        parser.add_argument(
            "--invalid-rows",
            type=int,
            default=0,
            help="How many intentionally invalid rows to include (default: 0)",
        )

        parser.add_argument(
            "--end-date",
            type=str,
            default=None,
            help="End date YYYY-MM-DD (default: today)",
        )

        parser.add_argument(
            "--out",
            type=str,
            default="data/sample.csv",
            help="Output path relative to BASE_DIR (default: data/sample.csv)",
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

        stations = [s.strip() for s in options["stations"] if s.strip()]
        if len(stations) < 3:
            raise CommandError("--stations must include at least 3 station IDs")

        days = options["days"]
        if days <= 0:
            raise CommandError("--days must be > 0")

        invalid_rows = options["invalid_rows"]
        if invalid_rows < 0:
            raise CommandError("--invalid-rows must be >= 0")
        if invalid_rows > rows:
            raise CommandError("--invalid-rows cannot exceed --rows")

        random.seed(options["seed"])

        base_dir = Path(settings.BASE_DIR)  # e.g. /app in container
        out_path = (base_dir / options["out"]).resolve()
        if base_dir not in out_path.parents and out_path != base_dir:
            raise CommandError("Output path must be inside BASE_DIR")
        out_path.parent.mkdir(parents=True, exist_ok=True)

        # End date
        if options["end_date"]:
            try:
                y, m, d = map(int, options["end_date"].split("-"))
                end = date(y, m, d)
            except Exception:
                raise CommandError("--end-date must be YYYY-MM-DD")
        else:
            end = date.today()

        start = end - timedelta(days=days - 1)

        # Pick which row indices will be invalid
        invalid_indices = set(random.sample(range(rows), k=invalid_rows))

        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "station_id", "temp_c", "precip_mm"])

            for i in range(rows):
                station_id = random.choice(stations)
                dt = start + timedelta(days=random.randint(0, days - 1))

                # Mostly-valid values
                temp_c = round(random.uniform(-10, 35), 1)
                precip_mm = round(min(max(0.0, random.gauss(2.0, 4.0)), 500.0), 1)

                if i in invalid_indices:
                    # 👶 "bad rows" bucket: pick one kind of wrong thing
                    kind = random.choice(["bad_date", "missing_station", "temp_oob", "precip_oob", "not_number"])
                    if kind == "bad_date":
                        writer.writerow(["not-a-date", station_id, temp_c, precip_mm])
                        continue
                    if kind == "missing_station":
                        writer.writerow([dt.isoformat(), "", temp_c, precip_mm])
                        continue
                    if kind == "temp_oob":
                        writer.writerow([dt.isoformat(), station_id, 999, precip_mm])
                        continue
                    if kind == "precip_oob":
                        writer.writerow([dt.isoformat(), station_id, temp_c, 9999])
                        continue
                    if kind == "not_number":
                        writer.writerow([dt.isoformat(), station_id, "lol", "wat"])
                        continue

                writer.writerow([dt.isoformat(), station_id, temp_c, precip_mm])

        self.stdout.write(
            self.style.SUCCESS(
                f"✅ Wrote {rows} rows to {out_path} "
                f"(days={days}, stations={len(stations)}, invalid_rows={invalid_rows})"
            )
        )
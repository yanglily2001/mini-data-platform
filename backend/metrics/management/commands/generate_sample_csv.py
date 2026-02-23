import csv
import random
from datetime import date, timedelta
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Generate a sample CSV file with columns: date,station_id,temp_c,precip_mm"

    def add_arguments(self, parser):
        parser.add_argument("--rows", type=int, default=1000, help="Total rows to generate")
        parser.add_argument("--days", type=int, default=60, help="Date range (default: 60 days)")
        parser.add_argument(
            "--stations",
            nargs="+",
            default=["STATION_A", "STATION_B", "STATION_C"],
            help="Space-separated station IDs (default: STATION_A STATION_B STATION_C)",
        )
        parser.add_argument(
            "--invalid-rows",
            type=int,
            default=20,
            help="How many intentionally invalid rows to inject",
        )
        parser.add_argument(
            "--start-date",
            type=str,
            default=None,
            help="Optional start date YYYY-MM-DD. If omitted, uses today-(days-1).",
        )
        parser.add_argument(
            "--out",
            type=str,
            default="data/sample.csv",
            help="Output path relative to BASE_DIR (default: data/sample.csv)",
        )
        parser.add_argument("--seed", type=int, default=42, help="Random seed")

    def handle(self, *args, **options):
        rows = options["rows"]
        days = options["days"]
        stations = options["stations"]
        invalid_rows = options["invalid_rows"]
        seed = options["seed"]

        if rows <= 0:
            raise CommandError("--rows must be > 0")
        if days <= 0:
            raise CommandError("--days must be > 0")
        if not stations:
            raise CommandError("--stations must include at least one station id")
        if invalid_rows < 0:
            raise CommandError("--invalid-rows must be >= 0")
        if invalid_rows > rows:
            raise CommandError("--invalid-rows cannot exceed --rows")

        random.seed(seed)

        base_dir = Path(settings.BASE_DIR)
        out_path = (base_dir / options["out"]).resolve()
        if base_dir not in out_path.parents and out_path != base_dir:
            raise CommandError("Output path must be inside BASE_DIR")
        out_path.parent.mkdir(parents=True, exist_ok=True)

        # date range
        if options["start_date"]:
            try:
                y, m, d = map(int, options["start_date"].split("-"))
                start = date(y, m, d)
            except Exception:
                raise CommandError("--start-date must be YYYY-MM-DD")
        else:
            start = date.today() - timedelta(days=days - 1)

        date_choices = [start + timedelta(days=i) for i in range(days)]

        # Pick which row indices will be invalid
        invalid_idxs = set(random.sample(range(rows), k=invalid_rows))

        def gen_valid_row():
            dt = random.choice(date_choices)
            station_id = random.choice(stations)

            # “real-ish” temp & precip
            temp_c = round(random.uniform(-10, 35), 1)
            precip_mm = round(max(0.0, random.gauss(2.0, 4.0)), 1)
            precip_mm = min(precip_mm, 500.0)

            return dt.isoformat(), station_id, temp_c, precip_mm

        def gen_invalid_row():
            # Rotate through a few kinds of invalidness
            kind = random.choice(["bad_date", "missing_station", "temp_oob", "precip_oob"])
            dt, station_id, temp_c, precip_mm = gen_valid_row()

            if kind == "bad_date":
                dt = "not-a-date"
            elif kind == "missing_station":
                station_id = ""
            elif kind == "temp_oob":
                temp_c = 999  # outside -60..60
            elif kind == "precip_oob":
                precip_mm = 9999  # outside 0..500

            return dt, station_id, temp_c, precip_mm

        with out_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "station_id", "temp_c", "precip_mm"])

            for i in range(rows):
                if i in invalid_idxs:
                    w.writerow(gen_invalid_row())
                else:
                    w.writerow(gen_valid_row())

        self.stdout.write(
            self.style.SUCCESS(
                f"✅ Wrote {rows} rows to {out_path} "
                f"(days={days}, stations={len(stations)}, invalid_rows={invalid_rows})"
            )
        )
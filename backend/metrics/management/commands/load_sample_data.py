from pathlib import Path
import csv

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.dateparse import parse_date

from metrics.models import Measurement, StagingMeasurement


class Command(BaseCommand):
    help = "Load backend/data/sample.csv into staging + measurements (staging truncated; measurements upserted)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--path",
            default=None,
            help="Optional path to CSV (defaults to backend/data/sample.csv).",
        )
        parser.add_argument(
            "--keep-staging",
            action="store_true",
            help="Do not truncate staging before loading.",
        )

    def handle(self, *args, **options):
        # Default CSV path: backend/data/sample.csv
        if options["path"]:
            csv_path = Path(options["path"])
        else:
            # This command runs with cwd=backend typically; but be safe:
            # <repo>/backend/metrics/management/commands/load_sample_data.py -> parents[4] is <repo>/backend
            backend_dir = Path(__file__).resolve().parents[4]
            csv_path = backend_dir / "backend" / "data" / "sample.csv"

        if not csv_path.exists():
            self.stderr.write(self.style.ERROR(f"CSV not found: {csv_path}"))
            return

        required_cols = {"date", "station_id", "temp_c", "precip_mm"}

        rows_in_file = 0
        staging_rows = []
        clean_rows = []

        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None or not required_cols.issubset(set(reader.fieldnames)):
                self.stderr.write(
                    self.style.ERROR(
                        f"CSV must include columns: {sorted(required_cols)}. Found: {reader.fieldnames}"
                    )
                )
                return

            for raw in reader:
                rows_in_file += 1

                date = parse_date((raw.get("date") or "").strip())  # YYYY-MM-DD
                station_id = (raw.get("station_id") or "").strip() or None

                def parse_float(v):
                    v = (v or "").strip()
                    if v == "":
                        return None
                    try:
                        return float(v)
                    except ValueError:
                        return None

                temp_c = parse_float(raw.get("temp_c"))
                precip_mm = parse_float(raw.get("precip_mm"))

                staging_rows.append(
                    StagingMeasurement(
                        date=date,
                        station_id=station_id,
                        temp_c=temp_c,
                        precip_mm=precip_mm,
                    )
                )

                # Basic “accept only fully-parseable + non-empty station_id” rule for sample load
                # (Your import endpoint can be stricter; this is just a simple loader.)
                if date and station_id and temp_c is not None and precip_mm is not None:
                    clean_rows.append(
                        Measurement(
                            date=date,
                            station_id=station_id,
                            temp_c=temp_c,
                            precip_mm=precip_mm,
                        )
                    )

        with transaction.atomic():
            if not options["keep_staging"]:
                StagingMeasurement.objects.all().delete()

            if staging_rows:
                StagingMeasurement.objects.bulk_create(staging_rows, batch_size=2000)

            upserted = 0
            if clean_rows:
                Measurement.objects.bulk_create(
                    clean_rows,
                    batch_size=2000,
                    update_conflicts=True,
                    unique_fields=["station_id", "date"],
                    update_fields=["temp_c", "precip_mm"],
                )
                upserted = len(clean_rows)

        self.stdout.write(self.style.SUCCESS("✅ Sample data loaded"))
        self.stdout.write(f"rows_in_file={rows_in_file}")
        self.stdout.write(f"rows_staged={len(staging_rows)}")
        self.stdout.write(f"rows_upserted={upserted}")
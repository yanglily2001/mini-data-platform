import csv
from datetime import datetime
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from metrics.models import Measurement, StagingMeasurement

TEMP_MIN_C = -60.0
TEMP_MAX_C = 60.0
PRECIP_MIN_MM = 0.0
PRECIP_MAX_MM = 500.0


def _parse_date(value: str):
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _parse_float(value: str):
    if value is None:
        return None
    value = value.strip()
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _validate_row(date, station_id, temp_c, precip_mm):
    errors = []

    if date is None:
        errors.append("invalid_date")
    if not station_id:
        errors.append("missing_station_id")

    if temp_c is None:
        errors.append("invalid_temp_c")
    elif temp_c < TEMP_MIN_C or temp_c > TEMP_MAX_C:
        errors.append("temp_c_out_of_range")

    if precip_mm is None:
        errors.append("invalid_precip_mm")
    elif precip_mm < PRECIP_MIN_MM or precip_mm > PRECIP_MAX_MM:
        errors.append("precip_mm_out_of_range")

    return (len(errors) == 0, errors)


class Command(BaseCommand):
    help = "Load data/sample.csv into staging and upsert valid rows into measurements."

    def add_arguments(self, parser):
        parser.add_argument(
            "--path",
            type=str,
            default="data/sample.csv",
            help="Path to CSV relative to BASE_DIR (default: data/sample.csv)",
        )

    def handle(self, *args, **options):
        rel_path = options["path"]
        csv_path = Path(settings.BASE_DIR) / rel_path  # ✅ works in host + docker

        if not csv_path.exists():
            self.stderr.write(f"CSV not found: {csv_path}")
            return

        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)

            required = {"date", "station_id", "temp_c", "precip_mm"}
            if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
                self.stderr.write(f"CSV must include columns: {sorted(required)}. Found: {reader.fieldnames}")
                return

            staging_rows = []
            valid_rows = []
            rows_in_file = 0
            rows_valid = 0

            for raw in reader:
                rows_in_file += 1

                date = _parse_date(raw.get("date"))
                station_id = (raw.get("station_id") or "").strip() or None
                temp_c = _parse_float(raw.get("temp_c"))
                precip_mm = _parse_float(raw.get("precip_mm"))

                is_valid, _errors = _validate_row(date, station_id, temp_c, precip_mm)

                staging_rows.append(
                    StagingMeasurement(
                        date=date,
                        station_id=station_id,
                        temp_c=temp_c,
                        precip_mm=precip_mm,
                    )
                )

                if is_valid:
                    rows_valid += 1
                    valid_rows.append(
                        Measurement(
                            date=date,
                            station_id=station_id,
                            temp_c=temp_c,
                            precip_mm=precip_mm,
                        )
                    )

        with transaction.atomic():
            # ✅ idempotent: staging truncated each run
            StagingMeasurement.objects.all().delete()
            if staging_rows:
                StagingMeasurement.objects.bulk_create(staging_rows, batch_size=2000)

            # ✅ idempotent: upsert on (station_id, date)
            if valid_rows:
                Measurement.objects.bulk_create(
                    valid_rows,
                    batch_size=2000,
                    update_conflicts=True,
                    unique_fields=["station_id", "date"],
                    update_fields=["temp_c", "precip_mm"],
                )

        rows_invalid = rows_in_file - rows_valid
        self.stdout.write(
            f"Loaded sample CSV ✅ rows_in_file={rows_in_file}, rows_valid={rows_valid}, rows_invalid={rows_invalid}"
        )
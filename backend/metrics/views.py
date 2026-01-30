cat > metrics/views.py <<'EOF'
import csv
import io
from datetime import datetime

from django.db import transaction
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from .models import Measurement, StagingMeasurement


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


def _is_valid_row(date, station_id, temp_c, precip_mm):
    # Basic validation rules (adjust as needed)
    if date is None:
        return False
    if station_id is None or station_id.strip() == "":
        return False
    if temp_c is None or not (-90.0 <= temp_c <= 60.0):
        return False
    if precip_mm is None or precip_mm < 0.0:
        return False
    return True


@csrf_exempt
@require_POST
def import_csv(request):
    """
    POST /api/import/
    multipart/form-data with file field name: 'file'

    Returns:
      {"rows_in_file":..., "rows_valid":..., "rows_invalid":..., "rows_upserted":...}
    """
    uploaded = request.FILES.get("file")
    if not uploaded:
        return JsonResponse({"error": "Missing file field 'file'."}, status=400)

    try:
        text = uploaded.read().decode("utf-8-sig")  # handles BOM too
    except UnicodeDecodeError:
        return JsonResponse({"error": "File must be UTF-8 encoded CSV."}, status=400)

    reader = csv.DictReader(io.StringIO(text))
    required_cols = {"date", "station_id", "temp_c", "precip_mm"}
    if reader.fieldnames is None or not required_cols.issubset(set(reader.fieldnames)):
        return JsonResponse(
            {
                "error": "CSV must include columns: date, station_id, temp_c, precip_mm",
                "found": reader.fieldnames,
            },
            status=400,
        )

    rows_in_file = 0
    staging_rows = []
    valid_clean_rows = []

    for raw in reader:
        rows_in_file += 1

        date = _parse_date(raw.get("date"))
        station_id = (raw.get("station_id") or "").strip() or None
        temp_c = _parse_float(raw.get("temp_c"))
        precip_mm = _parse_float(raw.get("precip_mm"))

        staging_rows.append(
            StagingMeasurement(
                date=date,
                station_id=station_id,
                temp_c=temp_c,
                precip_mm=precip_mm,
            )
        )

        if _is_valid_row(date, station_id, temp_c, precip_mm):
            valid_clean_rows.append(
                Measurement(
                    date=date,
                    station_id=station_id,
                    temp_c=temp_c,
                    precip_mm=precip_mm,
                )
            )

    rows_valid = len(valid_clean_rows)
    rows_invalid = rows_in_file - rows_valid

    with transaction.atomic():
        # clear staging each import (simple approach)
        StagingMeasurement.objects.all().delete()

        if staging_rows:
            StagingMeasurement.objects.bulk_create(staging_rows, batch_size=2000)

        rows_upserted = 0
        if valid_clean_rows:
            Measurement.objects.bulk_create(
                valid_clean_rows,
                batch_size=2000,
                update_conflicts=True,
                unique_fields=["station_id", "date"],
                update_fields=["temp_c", "precip_mm"],
            )
            rows_upserted = len(valid_clean_rows)

    return JsonResponse(
        {
            "rows_in_file": rows_in_file,
            "rows_valid": rows_valid,
            "rows_invalid": rows_invalid,
            "rows_upserted": rows_upserted,
        }
    )
EOF

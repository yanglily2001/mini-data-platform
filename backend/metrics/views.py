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

from django.db.models import Count, Min, Max, Q


def _null_count(qs, field_name: str) -> int:
    # Counts NULLs. (Not counting empty strings; add that if station_id can be "")
    return qs.filter(**{f"{field_name}__isnull": True}).count()


def _duplicate_rows_count(qs) -> int:
    """
    Count *extra* duplicate rows by natural key (station_id, date).
    Example: if a key appears 3 times, that's 2 duplicates.
    """
    dup_groups = (
        qs.values("station_id", "date")
        .annotate(n=Count("id"))
        .filter(n__gt=1)
    )

    # Sum (n - 1) across duplicate groups:
    return sum(g["n"] - 1 for g in dup_groups)


def _out_of_range_counts(qs) -> dict:
    """
    Mirrors your import validation:
      temp_c must be between -90 and 60 inclusive
      precip_mm must be >= 0
    Counts exclude NULLs so that "null" and "out-of-range" aren't double-counted.
    """
    return {
        "temp_c_lt_-90": qs.filter(temp_c__isnull=False, temp_c__lt=-90.0).count(),
        "temp_c_gt_60": qs.filter(temp_c__isnull=False, temp_c__gt=60.0).count(),
        "precip_mm_lt_0": qs.filter(precip_mm__isnull=False, precip_mm__lt=0.0).count(),
    }


def quality_report_for_queryset(qs, *, fields_for_nulls: list[str]) -> dict:
    total = qs.count()

    # Null counts + null rates
    null_counts = {f: _null_count(qs, f) for f in fields_for_nulls}
    null_rates = {
        f: (null_counts[f] / total if total else 0.0)
        for f in fields_for_nulls
    }

    # Min/max numeric
    agg = qs.aggregate(
        temp_c_min=Min("temp_c"),
        temp_c_max=Max("temp_c"),
        precip_mm_min=Min("precip_mm"),
        precip_mm_max=Max("precip_mm"),
    )

    duplicates_extra_rows = _duplicate_rows_count(qs)
    out_of_range = _out_of_range_counts(qs)

    return {
        "total_rows": total,
        "null_counts": null_counts,
        "null_rates": null_rates,
        "duplicates": {
            "key": ["station_id", "date"],
            "extra_duplicate_rows": duplicates_extra_rows,
        },
        "min_max": {
            "temp_c": {"min": agg["temp_c_min"], "max": agg["temp_c_max"]},
            "precip_mm": {"min": agg["precip_mm_min"], "max": agg["precip_mm_max"]},
        },
        "out_of_range_counts": out_of_range,
    }


def quality_report(request):
    """
    GET /api/quality/?table=measurements|staging
    Defaults to measurements.
    """
    from .models import Measurement, StagingMeasurement

    table = (request.GET.get("table") or "measurements").strip().lower()
    if table not in ("measurements", "staging"):
        return JsonResponse({"error": "table must be 'measurements' or 'staging'."}, status=400)

    if table == "staging":
        qs = StagingMeasurement.objects.all()
    else:
        qs = Measurement.objects.all()

    data = quality_report_for_queryset(
        qs,
        fields_for_nulls=["date", "station_id", "temp_c", "precip_mm"],
    )
    data["table"] = table
    return JsonResponse(data)

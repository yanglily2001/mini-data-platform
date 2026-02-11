import csv
import io
from datetime import datetime

from django.db import transaction
from django.http import JsonResponse
from django.db.models import Avg, Sum
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.views.decorators.http import require_GET

from .models import Measurement, StagingMeasurement

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
    """
    Returns (is_valid: bool, errors: list[str])
    """
    errors = []

    if date is None:
        errors.append("invalid_date")

    if station_id is None or station_id.strip() == "":
        errors.append("missing_station_id")

    if temp_c is None:
        errors.append("invalid_temp_c")
    else:
        if temp_c < TEMP_MIN_C or temp_c > TEMP_MAX_C:
            errors.append("temp_c_out_of_range")

    if precip_mm is None:
        errors.append("invalid_precip_mm")
    else:
        if precip_mm < PRECIP_MIN_MM or precip_mm > PRECIP_MAX_MM:
            errors.append("precip_mm_out_of_range")

    return (len(errors) == 0, errors)

@csrf_exempt
@require_POST
def import_csv(request):
    ...
    rows_in_file = 0
    staging_rows = []
    valid_clean_rows = []

    for raw in reader:
        rows_in_file += 1
        ... parse + validate ...
        staging_rows.append(StagingMeasurement(...))
        if is_valid:
            valid_clean_rows.append(Measurement(...))

    rows_valid = len(valid_clean_rows)
    rows_invalid = rows_in_file - rows_valid

    with transaction.atomic():
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

    return JsonResponse({
        "rows_in_file": rows_in_file,
        "rows_valid": rows_valid,
        "rows_invalid": rows_invalid,
        "rows_upserted": rows_upserted,
    })
    
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

@require_GET
def daily_metrics(request):
    station_id = (request.GET.get("station_id") or "").strip()
    if not station_id:
        return JsonResponse({"error": "Missing required query param: station_id"}, status=400)

    qs = (
        Measurement.objects
        .filter(station_id=station_id)
        .order_by("date")
        .values("date", "temp_c", "precip_mm")
    )

    # Convert date objects to ISO strings for JSON/charting
    data = [
        {
            "date": row["date"].isoformat() if row["date"] else None,
            "temp_c": row["temp_c"],
            "precip_mm": row["precip_mm"],
        }
        for row in qs
    ]

    return JsonResponse(
        {
            "station_id": station_id,
            "count": len(data),
            "data": data,
        }
    )

def _parse_iso_date(value: str):
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None

@require_GET
def stations(request):
    qs = (
        Measurement.objects
        .values_list("station_id", flat=True)
        .distinct()
        .order_by("station_id")
    )
    return JsonResponse({"stations": list(qs)})

@require_GET
def metrics_summary(request):
    """
    GET /api/metrics/summary/?station_id=X&from=YYYY-MM-DD&to=YYYY-MM-DD

    Returns:
      {
        "station_id": "X",
        "from": "YYYY-MM-DD",
        "to": "YYYY-MM-DD",
        "days": <count_of_rows>,
        "avg_temp_c": <float_or_null>,
        "total_precip_mm": <float_or_null>
      }
    """
    station_id = (request.GET.get("station_id") or "").strip()
    date_from = _parse_iso_date(request.GET.get("from"))
    date_to = _parse_iso_date(request.GET.get("to"))

    if not station_id:
        return JsonResponse({"error": "Missing required query param: station_id"}, status=400)
    if date_from is None:
        return JsonResponse({"error": "Missing or invalid required query param: from (YYYY-MM-DD)"}, status=400)
    if date_to is None:
        return JsonResponse({"error": "Missing or invalid required query param: to (YYYY-MM-DD)"}, status=400)
    if date_from > date_to:
        return JsonResponse({"error": "'from' must be <= 'to'."}, status=400)

    qs = Measurement.objects.filter(
        station_id=station_id,
        date__gte=date_from,
        date__lte=date_to,
    )

    agg = qs.aggregate(
        avg_temp_c=Avg("temp_c"),
        total_precip_mm=Sum("precip_mm"),
    )

    return JsonResponse(
        {
            "station_id": station_id,
            "from": date_from.isoformat(),
            "to": date_to.isoformat(),
            "days": qs.count(),
            "avg_temp_c": agg["avg_temp_c"],
            "total_precip_mm": agg["total_precip_mm"],
        }
    )

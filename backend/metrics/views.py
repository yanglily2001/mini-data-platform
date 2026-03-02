import csv
import io
from datetime import date as date_type
from typing import Optional

from django.db import transaction
from django.http import JsonResponse, StreamingHttpResponse, HttpResponseBadRequest
from django.utils.dateparse import parse_date
from django.db.models import Avg, Sum, Count
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.views.decorators.http import require_GET

from metrics.models import Measurement, StagingMeasurement

from rest_framework.views import APIView

TEMP_MIN_C = -60.0
TEMP_MAX_C = 60.0
PRECIP_MIN_MM = 0.0
PRECIP_MAX_MM = 500.0
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000

class Echo:
    """A file-like object that just returns what you write to it (for streaming)."""
    def write(self, value):
        return value

def metrics_download_csv(request):
    station_id = request.GET.get("station_id")
    date_from = request.GET.get("from")
    date_to = request.GET.get("to")

    qs = Measurement.objects.all().order_by("date", "station_id")

    if station_id:
        qs = qs.filter(station_id=station_id)

    if date_from:
        d1 = parse_date(date_from)
        if not d1:
            return HttpResponseBadRequest('{"error":"invalid from date"}', content_type="application/json")
        qs = qs.filter(date__gte=d1)

    if date_to:
        d2 = parse_date(date_to)
        if not d2:
            return HttpResponseBadRequest('{"error":"invalid to date"}', content_type="application/json")
        qs = qs.filter(date__lte=d2)

    # Stream rows to avoid loading everything in memory
    pseudo_buffer = Echo()
    writer = csv.writer(pseudo_buffer)

    def row_iter():
        yield writer.writerow(["date", "station_id", "temp_c", "precip_mm"])
        for m in qs.iterator(chunk_size=2000):
            yield writer.writerow([m.date.isoformat(), m.station_id, m.temp_c, m.precip_mm])

    filename_parts = ["metrics"]
    if station_id:
        filename_parts.append(station_id)
    if date_from:
        filename_parts.append(f"from_{date_from}")
    if date_to:
        filename_parts.append(f"to_{date_to}")
    filename = "_".join(filename_parts) + ".csv"

    resp = StreamingHttpResponse(row_iter(), content_type="text/csv; charset=utf-8")
    resp["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp

def _parse_date(param: Optional[str], name: str) -> Optional[date]:
    if not param:
        return None
    try:
        return date.fromisoformat(param)
    except ValueError:
        raise ValueError(f"{name} must be YYYY-MM-DD")

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
    uploaded = request.FILES.get("file")
    if not uploaded:
        return JsonResponse({"error": "Missing file field 'file'."}, status=400)

    try:
        text = uploaded.read().decode("utf-8-sig")
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
    valid_rows = []
    invalid_rows = 0

    for raw in reader:
        rows_in_file += 1

        date_val = _parse_date(raw.get("date"))
        station_id = (raw.get("station_id") or "").strip() or None
        temp_c = _parse_float(raw.get("temp_c"))
        precip_mm = _parse_float(raw.get("precip_mm"))

        is_valid, errors = _validate_row(date_val, station_id, temp_c, precip_mm)

        staging_rows.append(
            StagingMeasurement(
                date=date_val,
                station_id=station_id,
                temp_c=temp_c,
                precip_mm=precip_mm,
            )
        )

        if is_valid:
            valid_rows.append(
                Measurement(
                    date=date_val,
                    station_id=station_id,
                    temp_c=temp_c,
                    precip_mm=precip_mm,
                )
            )
        else:
            invalid_rows += 1

    # ✅ DEDUPE valid rows by (station_id, date) BEFORE bulk upsert
    # If duplicates exist in the file, last one wins (common & reasonable).
    deduped = {}
    for m in valid_rows:
        deduped[(m.station_id, m.date)] = m
    valid_rows = list(deduped.values())

    rows_valid = len(valid_rows)
    rows_invalid = invalid_rows

    with transaction.atomic():
        # staging contains ALL rows (even invalid)
        StagingMeasurement.objects.all().delete()
        if staging_rows:
            StagingMeasurement.objects.bulk_create(staging_rows, batch_size=2000)

        rows_upserted = 0
        if valid_rows:
            Measurement.objects.bulk_create(
                valid_rows,
                batch_size=2000,
                update_conflicts=True,
                unique_fields=["station_id", "date"],
                update_fields=["temp_c", "precip_mm"],
            )
            rows_upserted = len(valid_rows)

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

@require_GET
def daily_metrics(request):
    station_id = request.GET.get("station_id")
    if not station_id:
        return JsonResponse({"error": "station_id is required"}, status=400)

    # pagination params
    try:
        limit = int(request.GET.get("limit", DEFAULT_LIMIT))
        offset = int(request.GET.get("offset", 0))
    except ValueError:
        return JsonResponse({"error": "limit and offset must be integers"}, status=400)

    if limit < 1:
        return JsonResponse({"error": "limit must be >= 1"}, status=400)
    if offset < 0:
        return JsonResponse({"error": "offset must be >= 0"}, status=400)

    limit = min(limit, MAX_LIMIT)

    # build your base queryset (adjust to your real logic)
    qs = (
        Measurement.objects
        .filter(station_id=station_id)
        .order_by("date")
        .values("date")
        .annotate(
            temp_c=Avg("temp_c"),
            precip_mm=Avg("precip_mm"),
        )
    )

    total = qs.count()

    page_qs = qs[offset: offset + limit]
    data = [
        {"date": r["date"].isoformat(), "temp_c": float(r["temp_c"]), "precip_mm": float(r["precip_mm"])}
        for r in page_qs
    ]

    return JsonResponse({
        "station_id": station_id,
        "count": total,          # total matching rows (not page size)
        "limit": limit,
        "offset": offset,
        "next_offset": (offset + limit) if (offset + limit) < total else None,
        "data": data,
    })

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

    - station_id required
    - from/to optional
    - If from/to omitted, computes over ALL dates for that station

    Returns:
      {
        "station_id": "X",
        "from": "YYYY-MM-DD" | null,
        "to": "YYYY-MM-DD" | null,
        "total_rows": <int>,
        "avg_temp_c": <float|null>,
        "total_precip_mm": <float>
      }
    """
    station_id = (request.GET.get("station_id") or "").strip()
    date_from = _parse_iso_date(request.GET.get("from"))
    date_to = _parse_iso_date(request.GET.get("to"))

    if not station_id:
        return JsonResponse({"error": "Missing required query param: station_id"}, status=400)

    # If user provided from/to but parsing failed, tell them
    if request.GET.get("from") and date_from is None:
        return JsonResponse({"error": "Invalid 'from' date. Use YYYY-MM-DD"}, status=400)
    if request.GET.get("to") and date_to is None:
        return JsonResponse({"error": "Invalid 'to' date. Use YYYY-MM-DD"}, status=400)

    if date_from and date_to and date_from > date_to:
        return JsonResponse({"error": "'from' must be <= 'to'."}, status=400)

    qs = Measurement.objects.filter(station_id=station_id)

    if date_from:
        qs = qs.filter(date__gte=date_from)
    if date_to:
        qs = qs.filter(date__lte=date_to)

    agg = qs.aggregate(
        total_rows=Count("id"),
        avg_temp_c=Avg("temp_c"),
        total_precip_mm=Sum("precip_mm"),
    )

    total_rows = agg["total_rows"] or 0
    avg_temp_c = agg["avg_temp_c"]
    total_precip_mm = agg["total_precip_mm"]

    return JsonResponse(
        {
            "station_id": station_id,
            "from": date_from.isoformat() if date_from else None,
            "to": date_to.isoformat() if date_to else None,
            "total_rows": total_rows,
            "avg_temp_c": float(avg_temp_c) if avg_temp_c is not None else None,
            "total_precip_mm": float(total_precip_mm) if total_precip_mm is not None else 0.0,
        }
    )


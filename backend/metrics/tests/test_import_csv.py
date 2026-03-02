from django.test import TestCase, Client
from django.core.files.uploadedfile import SimpleUploadedFile
from datetime import date, timedelta

from metrics.models import Measurement, StagingMeasurement


class ImportCsvTests(TestCase):
    def setUp(self):
        self.client = Client()

    def _post_csv(self, csv_text: str):
        upload = SimpleUploadedFile(
            "test.csv",
            csv_text.encode("utf-8"),
            content_type="text/csv",
        )
        return self.client.post("/api/import/", data={"file": upload})

    def test_import_inserts_valid_rows_and_counts(self):
        csv_text = (
            "date,station_id,temp_c,precip_mm\n"
            "2026-01-01,STATION_A,10,0\n"
            "bad-date,STATION_A,10,0\n"          # invalid date
            "2026-01-02,,10,0\n"                 # missing station_id
            "2026-01-03,STATION_A,999,0\n"       # temp out of range
            "2026-01-04,STATION_A,10,9999\n"     # precip out of range (per your validation)
        )

        resp = self._post_csv(csv_text)
        self.assertEqual(resp.status_code, 200)

        data = resp.json()
        self.assertEqual(data["rows_in_file"], 5)
        self.assertEqual(data["rows_valid"], 1)
        self.assertEqual(data["rows_invalid"], 4)
        self.assertEqual(data["rows_upserted"], 1)

        # Measurements should contain only the valid row
        self.assertEqual(Measurement.objects.count(), 1)
        m = Measurement.objects.get()
        self.assertEqual(m.station_id, "STATION_A")
        self.assertEqual(str(m.date), "2026-01-01")
        self.assertEqual(float(m.temp_c), 10.0)
        self.assertEqual(float(m.precip_mm), 0.0)

        # Staging should contain all rows (even invalid) if your import does that
        self.assertEqual(StagingMeasurement.objects.count(), 5)

    def test_import_is_idempotent_upsert_updates_existing_row(self):
        csv_v1 = (
            "date,station_id,temp_c,precip_mm\n"
            "2026-01-01,STATION_A,10,0\n"
            "2026-01-02,STATION_A,11,1\n"
        )
        csv_v2 = (
            "date,station_id,temp_c,precip_mm\n"
            "2026-01-01,STATION_A,55,0\n"   # same key (station_id,date) but changed temp
            "2026-01-02,STATION_A,11,123\n" # same key but changed precip
        )

        r1 = self._post_csv(csv_v1)
        self.assertEqual(r1.status_code, 200)
        self.assertEqual(Measurement.objects.count(), 2)

        r2 = self._post_csv(csv_v2)
        self.assertEqual(r2.status_code, 200)

        # Still 2 rows total (no duplicates)
        self.assertEqual(Measurement.objects.count(), 2)

        rows = list(
            Measurement.objects.filter(station_id="STATION_A")
            .order_by("date")
            .values("date", "temp_c", "precip_mm")
        )

        self.assertEqual(str(rows[0]["date"]), "2026-01-01")
        self.assertEqual(float(rows[0]["temp_c"]), 55.0)
        self.assertEqual(float(rows[0]["precip_mm"]), 0.0)

        self.assertEqual(str(rows[1]["date"]), "2026-01-02")
        self.assertEqual(float(rows[1]["temp_c"]), 11.0)
        self.assertEqual(float(rows[1]["precip_mm"]), 123.0)

    def test_missing_file_returns_400(self):
        resp = self.client.post("/api/import/", data={})
        self.assertEqual(resp.status_code, 400)
        self.assertIn("error", resp.json())

def test_import_same_file_twice_is_idempotent(self):
    csv_text = (
        "date,station_id,temp_c,precip_mm\n"
        "2026-01-01,STATION_A,10,0\n"
        "2026-01-02,STATION_A,11,1\n"
    )

    r1 = self._post_csv(csv_text)
    self.assertEqual(r1.status_code, 200)
    self.assertEqual(Measurement.objects.count(), 2)

    r2 = self._post_csv(csv_text)
    self.assertEqual(r2.status_code, 200)

    # Still 2 rows total (no duplicates)
    self.assertEqual(Measurement.objects.count(), 2)

    rows = list(
        Measurement.objects.filter(station_id="STATION_A")
        .order_by("date")
        .values("date", "temp_c", "precip_mm")
    )
    self.assertEqual(str(rows[0]["date"]), "2026-01-01")
    self.assertEqual(float(rows[0]["temp_c"]), 10.0)
    self.assertEqual(float(rows[0]["precip_mm"]), 0.0)

def test_quality_endpoint_returns_expected_structure(self):
    csv_text = (
        "date,station_id,temp_c,precip_mm\n"
        "2026-01-01,STATION_A,10,0\n"
        "bad-date,STATION_A,10,0\n"
        "2026-01-02,,10,0\n"
        "2026-01-03,STATION_A,999,0\n"
        "2026-01-04,STATION_A,10,9999\n"
    )
    self._post_csv(csv_text)

    # quality on measurements (only valid rows should be present there)
    resp = self.client.get("/api/quality/?table=measurements")
    self.assertEqual(resp.status_code, 200)
    data = resp.json()

    # Key presence checks 
    self.assertIn("table", data)
    self.assertIn("total_rows", data)
    self.assertIn("null_counts", data)
    self.assertIn("null_rates", data)
    self.assertIn("duplicates", data)
    self.assertIn("min_max", data)
    self.assertIn("out_of_range_counts", data)

    self.assertEqual(data["table"], "measurements")
    self.assertEqual(data["total_rows"], 1)  # only one valid row from that CSV

    # Some value sanity checks 
    self.assertEqual(data["duplicates"]["key"], ["station_id", "date"])
    self.assertEqual(data["duplicates"]["extra_duplicate_rows"], 0)

    # min/max should match the single valid row
    self.assertEqual(float(data["min_max"]["temp_c"]["min"]), 10.0)
    self.assertEqual(float(data["min_max"]["temp_c"]["max"]), 10.0)
    self.assertEqual(float(data["min_max"]["precip_mm"]["min"]), 0.0)
    self.assertEqual(float(data["min_max"]["precip_mm"]["max"]), 0.0)

    def test_summary_endpoint_returns_correct_aggregates(self):
        # Arrange: import a tiny known dataset for one station
        csv_text = (
            "date,station_id,temp_c,precip_mm\n"
            "2026-01-01,STATION_A,10,1\n"
            "2026-01-02,STATION_A,20,2\n"
            "2026-01-03,STATION_A,30,3\n"
            "2026-01-01,STATION_B,999,0\n"  # invalid row (temp out of range) should NOT be inserted
        )
        r = self._post_csv(csv_text)
        self.assertEqual(r.status_code, 200)

        # Act: call summary for STATION_A across the imported date range
        resp = self.client.get(
            "/api/metrics/summary/",
            data={"station_id": "STATION_A", "from": "2026-01-01", "to": "2026-01-03"},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()

        # Assert: row count + aggregates
        # temps: 10, 20, 30 => avg = 20
        # precip: 1 + 2 + 3 => total = 6
        self.assertEqual(data["station_id"], "STATION_A")
        self.assertEqual(data["from"], "2026-01-01")
        self.assertEqual(data["to"], "2026-01-03")
        self.assertEqual(data["total_rows"], 3)

        self.assertAlmostEqual(float(data["avg_temp_c"]), 20.0, places=6)
        self.assertAlmostEqual(float(data["total_precip_mm"]), 6.0, places=6)

class DailyMetricsPaginationTests(TestCase):
    def setUp(self):
        self.client = Client()
        base = date(2026, 1, 1)
        for i in range(50):
            Measurement.objects.create(
                station_id="STATION_A",
                date=base + timedelta(days=i),
                temp_c=10.0,
                precip_mm=1.0,
            )

    def test_daily_metrics_paginates_with_limit_offset(self):
        r1 = self.client.get("/api/metrics/daily/?station_id=STATION_A&limit=10&offset=0")
        self.assertEqual(r1.status_code, 200)
        j1 = r1.json()
        self.assertEqual(j1["count"], 50)
        self.assertEqual(len(j1["data"]), 10)
        self.assertEqual(j1["offset"], 0)
        self.assertEqual(j1["limit"], 10)
        self.assertEqual(j1["next_offset"], 10)

        r2 = self.client.get("/api/metrics/daily/?station_id=STATION_A&limit=10&offset=10")
        self.assertEqual(r2.status_code, 200)
        j2 = r2.json()
        self.assertEqual(j2["count"], 50)
        self.assertEqual(len(j2["data"]), 10)
        self.assertEqual(j2["offset"], 10)

class DownloadCsvTests(TestCase):
    def setUp(self):
        self.client = Client()
        Measurement.objects.create(date=date(2026, 1, 1), station_id="STATION_A", temp_c=10, precip_mm=1)
        Measurement.objects.create(date=date(2026, 1, 2), station_id="STATION_A", temp_c=11, precip_mm=2)
        Measurement.objects.create(date=date(2026, 1, 1), station_id="STATION_B", temp_c=99, precip_mm=99)

    def test_download_csv_filters_by_station(self):
        resp = self.client.get("/api/metrics/download/?station_id=STATION_A")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp["Content-Type"].split(";")[0], "text/csv")

        body = b"".join(resp.streaming_content).decode("utf-8")
        lines = [line.strip() for line in body.splitlines() if line.strip()]

        # header + 2 rows
        self.assertEqual(lines[0], "date,station_id,temp_c,precip_mm")
        self.assertEqual(len(lines), 3)
        self.assertTrue(all(",STATION_A," in line or line.startswith("date,") for line in lines))

    def test_download_csv_date_range(self):
        resp = self.client.get("/api/metrics/download/?station_id=STATION_A&from=2026-01-02&to=2026-01-02")
        self.assertEqual(resp.status_code, 200)

        body = b"".join(resp.streaming_content).decode("utf-8")
        lines = [line.strip() for line in body.splitlines() if line.strip()]
        self.assertEqual(len(lines), 2)  # header + 1 row
        self.assertTrue(lines[1].startswith("2026-01-02,STATION_A,"))
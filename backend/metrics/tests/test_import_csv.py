from django.test import TestCase, Client
from django.core.files.uploadedfile import SimpleUploadedFile

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
            "2026-01-01,STATION_A,99,0\n"   # same key (station_id,date) but changed temp
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
        self.assertEqual(float(rows[0]["temp_c"]), 99.0)
        self.assertEqual(float(rows[0]["precip_mm"]), 0.0)

        self.assertEqual(str(rows[1]["date"]), "2026-01-02")
        self.assertEqual(float(rows[1]["temp_c"]), 11.0)
        self.assertEqual(float(rows[1]["precip_mm"]), 123.0)

    def test_missing_file_returns_400(self):
        resp = self.client.post("/api/import/", data={})
        self.assertEqual(resp.status_code, 400)
        self.assertIn("error", resp.json())

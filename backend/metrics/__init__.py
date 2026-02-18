from django.test import TestCase
from django.urls import reverse
from django.core.files.uploadedfile import SimpleUploadedFile

class ImportCsvTests(TestCase):
    def test_import_csv_counts_and_upserts(self):
        csv_bytes = b"""date,station_id,temp_c,precip_mm
2026-01-01,STATION_A,10,0
bad-date,STATION_A,10,0
2026-01-02,,10,0
2026-01-03,STATION_A,999,0
2026-01-04,STATION_A,10,9999
"""

        upload = SimpleUploadedFile(
            "validate_test.csv",
            csv_bytes,
            content_type="text/csv",
        )

        # If you don't use named urls, use the literal path:
        # url = "/api/import/"
        url = "/api/import/"

        resp = self.client.post(url, data={"file": upload})
        self.assertEqual(resp.status_code, 200)

        data = resp.json()
        self.assertEqual(data["rows_in_file"], 5)
        self.assertEqual(data["rows_valid"], 1)
        self.assertEqual(data["rows_invalid"], 4)
        self.assertEqual(data["rows_upserted"], 1)

        # staging contains all rows (even invalid)
        self.assertEqual(StagingMeasurement.objects.count(), 5)

        # measurement contains only valid rows
        self.assertEqual(Measurement.objects.count(), 1)

        m = Measurement.objects.get()
        self.assertEqual(m.station_id, "STATION_A")
        self.assertEqual(str(m.date), "2026-01-01")
        self.assertEqual(float(m.temp_c), 10.0)
        self.assertEqual(float(m.precip_mm), 0.0)

    def test_import_is_idempotent_upsert(self):
        csv1 = b"""date,station_id,temp_c,precip_mm
2026-01-01,STATION_A,10,0
"""
        csv2 = b"""date,station_id,temp_c,precip_mm
2026-01-01,STATION_A,99,5
"""

        url = "/api/import/"

        self.client.post(url, {"file": SimpleUploadedFile("a.csv", csv1, content_type="text/csv")})
        self.assertEqual(Measurement.objects.count(), 1)
        m1 = Measurement.objects.get(station_id="STATION_A", date="2026-01-01")
        self.assertEqual(float(m1.temp_c), 10.0)

        # re-import same natural key, different values -> should UPDATE (upsert)
        self.client.post(url, {"file": SimpleUploadedFile("b.csv", csv2, content_type="text/csv")})
        self.assertEqual(Measurement.objects.count(), 1)
        m2 = Measurement.objects.get(station_id="STATION_A", date="2026-01-01")
        self.assertEqual(float(m2.temp_c), 99.0)
        self.assertEqual(float(m2.precip_mm), 5.0)
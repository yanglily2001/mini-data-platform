from django.db import models


class StagingMeasurement(models.Model):
    """
    Raw rows loaded from CSV (may contain nulls/bad values).
    We'll truncate this table on every import.
    """
    date = models.DateField(null=True, blank=True)
    station_id = models.CharField(max_length=64, null=True, blank=True)

    # store raw numeric-ish values; allow nulls because staging is "dirty"
    temp_c = models.FloatField(null=True, blank=True)
    precip_mm = models.FloatField(null=True, blank=True)

    loaded_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "staging_measurements"
        indexes = [
            models.Index(fields=["station_id", "date"]),
        ]

    def __str__(self) -> str:
        return f"{self.station_id} {self.date} (staging)"


class Measurement(models.Model):
    """
    Clean fact table. Only validated rows get inserted/upserted here.
    Idempotency is enforced by unique (station_id, date).
    """
    date = models.DateField()
    station_id = models.CharField(max_length=64)

    temp_c = models.FloatField()
    precip_mm = models.FloatField()

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "measurements"
        constraints = [
            models.UniqueConstraint(
                fields=["station_id", "date"],
                name="uniq_measurements_station_date",
            )
        ]
        indexes = [
            models.Index(fields=["station_id", "date"]),
            models.Index(fields=["date"]),
        ]

    def __str__(self) -> str:
        return f"{self.station_id} {self.date}"

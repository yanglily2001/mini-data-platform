from django.db import models

class StagingMeasurement(models.Model):
    station_id = models.CharField(max_length=64, null=True, db_index=True)
    date = models.DateField(null=True, db_index=True)
    temp_c = models.FloatField(null=True)
    precip_mm = models.FloatField(null=True)

    class Meta:
        indexes = [
            models.Index(fields=["station_id"], name="idx_stage_station"),
            models.Index(fields=["date"], name="idx_stage_date"),
        ]

class Measurement(models.Model):
    station_id = models.CharField(max_length=64, db_index=True)
    date = models.DateField(db_index=True)
    temp_c = models.FloatField(null=True)
    precip_mm = models.FloatField(null=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["station_id", "date"], name="uniq_station_date"),
        ]
        indexes = [
            models.Index(fields=["station_id"], name="idx_meas_station"),
            models.Index(fields=["date"], name="idx_meas_date"),
            models.Index(fields=["station_id", "date"], name="idx_meas_station_date"),
        ]





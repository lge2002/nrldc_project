from django.db import models

class Table2A(models.Model):
    state = models.CharField(max_length=100)
    demand_forecast = models.FloatField(null=True, blank=True)
    demand_met = models.FloatField(null=True, blank=True)
    shortage = models.FloatField(null=True, blank=True)
    timestamp = models.DateField()

    def __str__(self):
        return f"{self.state} - {self.timestamp}"

class Table2C(models.Model):
    state = models.CharField(max_length=100)
    morning_peak = models.FloatField(null=True, blank=True)
    evening_peak = models.FloatField(null=True, blank=True)
    timestamp = models.DateField()

    def __str__(self):
        return f"{self.state} - {self.timestamp}"

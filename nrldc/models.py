from django.db import models

class NRDCReport(models.Model):
    report_date = models.DateField(unique=True)
    # Removed: download_url = models.URLField()
    # Removed: pdf_path = models.CharField(max_length=500)
    # Removed: report_title = models.CharField(max_length=255)
    # Removed: file_name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        # Updated __str__ as report_title is removed
        return f"Report for {self.report_date}"

    class Meta:
        verbose_name = "NRDC Report"
        verbose_name_plural = "NRDC Reports"

class Table2AData(models.Model):
    report = models.ForeignKey(NRDCReport, on_delete=models.CASCADE, related_name='table_2a_data')
    state = models.CharField(max_length=100, null=True, blank=True)
    thermal = models.FloatField(null=True, blank=True)
    hydro = models.FloatField(null=True, blank=True)
    gas_naptha_diesel = models.FloatField(null=True, blank=True)
    solar = models.FloatField(null=True, blank=True)
    wind = models.FloatField(null=True, blank=True)
    other_biomass_co_gen_etc = models.FloatField(null=True, blank=True)
    total_generation = models.FloatField(null=True, blank=True)
    drawal_sch_net_mu = models.FloatField(null=True, blank=True)
    act_drawal_net_mu = models.FloatField(null=True, blank=True)
    ui_net_mu = models.FloatField(null=True, blank=True)
    requirement_net_mu = models.FloatField(null=True, blank=True)
    shortage_net_mu = models.FloatField(null=True, blank=True)
    consumption_net_mu = models.FloatField(null=True, blank=True)

    def __str__(self):
        return f"Table 2A Data for {self.report.report_date} - {self.state}"

    class Meta:
        verbose_name = "Table 2A Data"
        verbose_name_plural = "Table 2A Data"


class Table2CData(models.Model):
    report = models.ForeignKey(NRDCReport, on_delete=models.CASCADE, related_name='table_2c_data')
    state = models.CharField(max_length=100, null=True, blank=True)

    max_demand_met_of_the_day = models.FloatField(null=True, blank=True)
    time_max_demand_met = models.CharField(max_length=50, null=True, blank=True)
    shortage_during_max_demand = models.FloatField(null=True, blank=True)
    requirement_at_max_demand = models.FloatField(null=True, blank=True)

    max_requirement_of_the_day = models.FloatField(null=True, blank=True)
    time_max_requirement = models.CharField(max_length=50, null=True, blank=True)
    shortage_during_max_requirement = models.FloatField(null=True, blank=True)
    demand_met_at_max_requirement = models.FloatField(null=True, blank=True)
    min_demand_met = models.FloatField(null=True, blank=True)
    time_min_demand_met = models.CharField(max_length=50, null=True, blank=True)
    unused_col = models.CharField(max_length=255, null=True, blank=True)

    ace_max = models.FloatField(null=True, blank=True)
    ace_min = models.FloatField(null=True, blank=True)
    time_ace = models.CharField(max_length=50, null=True, blank=True)

    def __str__(self):
        return f"Table 2C Data for {self.report.report_date} - {self.state}"

    class Meta:
        verbose_name = "Table 2C Data"
        verbose_name_plural = "Table 2C Data"
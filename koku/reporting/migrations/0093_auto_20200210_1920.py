# Generated by Django 2.2.10 on 2020-02-10 19:20
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [("reporting", "0092_auto_20200203_1758")]

    operations = [
        migrations.DeleteModel("AzureTagsSummary"),
        migrations.DeleteModel("AWSTagsSummary"),
        migrations.DeleteModel("OCPStorageVolumeClaimLabelSummary"),
        migrations.DeleteModel("OCPStorageVolumeLabelSummary"),
        migrations.DeleteModel("OCPUsagePodLabelSummary"),
    ]

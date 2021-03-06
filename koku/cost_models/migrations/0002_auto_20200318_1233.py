# Generated by Django 2.2.11 on 2020-03-18 12:33
import copy
import json

from django.db import migrations

from api.metrics import constants as metric_constants


def update_cost_model_rates(apps, schema_editor):
    """Update cost model rates with cost type."""
    CostModel = apps.get_model("cost_models", "CostModel")
    metrics = copy.deepcopy(metric_constants.COST_MODEL_METRIC_MAP)
    metric_dict = {metric.get("metric"): metric.get("default_cost_type") for metric in metrics}

    cost_models = CostModel.objects.all()

    for cost_model in cost_models:
        rates = cost_model.rates
        for rate in rates:
            metric = rate.get("metric", {}).get("name", "")
            # Add a default for older cost models
            if not rate.get("cost_type"):
                rate["cost_type"] = metric_dict.get(metric, metric_constants.SUPPLEMENTARY_COST_TYPE)
            # Move cost_type from tiered_rates to the rate level
            for tiered_rate in rate.get("tiered_rates", []):
                tiered_rate.pop("cost_type", "")
        cost_model.save()


class Migration(migrations.Migration):

    dependencies = [("cost_models", "0001_initial_squashed_0018_auto_20200116_2048")]

    operations = [migrations.RunPython(update_cost_model_rates)]

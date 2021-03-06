#
# Copyright 2020 Red Hat, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
"""Provider Mapper for GCP Reports."""
from django.contrib.postgres.aggregates import ArrayAgg
from django.db.models import DecimalField
from django.db.models import F
from django.db.models import Max
from django.db.models import Sum
from django.db.models import Value
from django.db.models.expressions import ExpressionWrapper
from django.db.models.functions import Coalesce

from api.models import Provider
from api.report.provider_map import ProviderMap
from reporting.provider.gcp.models import GCPCostEntryLineItemDailySummary
from reporting.provider.gcp.models import GCPCostSummary
from reporting.provider.gcp.models import GCPCostSummaryByAccount
from reporting.provider.gcp.models import GCPCostSummaryByProject
from reporting.provider.gcp.models import GCPCostSummaryByRegion
from reporting.provider.gcp.models import GCPCostSummaryByService


class GCPProviderMap(ProviderMap):
    """GCP Provider Map."""

    def __init__(self, provider, report_type):
        """Constructor."""
        self._mapping = [
            {
                "provider": Provider.PROVIDER_GCP,
                "annotations": {},  # Annotations that should always happen
                "group_by_annotations": {
                    "region": {"service": "service_alias"},
                    "account": {"account": "account_id"},
                    "project": {"project": "project_id"},
                    "service": {"service": "service_alias"},
                },  # Annotations that should happen depending on group_by values
                "end_date": "usage_end",
                "filters": {
                    "account": {"field": "account_id", "operation": "icontains"},
                    "region": {"field": "region", "operation": "icontains"},
                    "service": [
                        {"field": "service_alias", "operation": "icontains", "composition_key": "service_filter"},
                        {"field": "service_id", "operation": "icontains", "composition_key": "service_filter"},
                    ],
                    "project": [
                        {"field": "project_name", "operation": "icontains", "composition_key": "project_filter"},
                        {"field": "project_id", "operation": "icontains", "composition_key": "project_filter"},
                    ],
                },
                "group_by_options": ["account", "region", "service", "project"],
                "tag_column": "tags",
                "report_type": {
                    "costs": {
                        "aggregates": {
                            "infra_total": Sum(
                                Coalesce(F("unblended_cost"), Value(0, output_field=DecimalField()))
                                + Coalesce(F("markup_cost"), Value(0, output_field=DecimalField()))
                            ),
                            "infra_raw": Sum("unblended_cost"),
                            "infra_usage": Sum(Value(0, output_field=DecimalField())),
                            "infra_markup": Sum(Coalesce(F("markup_cost"), Value(0, output_field=DecimalField()))),
                            "sup_raw": Sum(Value(0, output_field=DecimalField())),
                            "sup_usage": Sum(Value(0, output_field=DecimalField())),
                            "sup_markup": Sum(Value(0, output_field=DecimalField())),
                            "sup_total": Sum(Value(0, output_field=DecimalField())),
                            "cost_total": Sum(
                                Coalesce(F("unblended_cost"), Value(0, output_field=DecimalField()))
                                + Coalesce(F("markup_cost"), Value(0, output_field=DecimalField()))
                            ),
                            "cost_raw": Sum("unblended_cost"),
                            "cost_usage": Sum(Value(0, output_field=DecimalField())),
                            "cost_markup": Sum(Coalesce(F("markup_cost"), Value(0, output_field=DecimalField()))),
                        },
                        "aggregate_key": "unblended_cost",
                        "annotations": {
                            "infra_raw": Sum("unblended_cost"),
                            "infra_usage": Value(0, output_field=DecimalField()),
                            "infra_markup": Sum(Coalesce(F("markup_cost"), Value(0, output_field=DecimalField()))),
                            "infra_total": Sum(
                                Coalesce(F("unblended_cost"), Value(0, output_field=DecimalField()))
                                + Coalesce(F("markup_cost"), Value(0, output_field=DecimalField()))
                            ),
                            "sup_raw": Value(0, output_field=DecimalField()),
                            "sup_usage": Value(0, output_field=DecimalField()),
                            "sup_markup": Value(0, output_field=DecimalField()),
                            "sup_total": Value(0, output_field=DecimalField()),
                            "cost_raw": Sum(Coalesce(F("unblended_cost"), Value(0, output_field=DecimalField()))),
                            "cost_usage": Value(0, output_field=DecimalField()),
                            "cost_markup": Sum(Coalesce(F("markup_cost"), Value(0, output_field=DecimalField()))),
                            "cost_total": Sum(
                                Coalesce(F("unblended_cost"), Value(0, output_field=DecimalField()))
                                + Coalesce(F("markup_cost"), Value(0, output_field=DecimalField()))
                            ),
                            "cost_units": Coalesce(Max("currency"), Value("USD")),
                            "source_uuid": ArrayAgg(F("source_uuid"), distinct=True),
                        },
                        "delta_key": {
                            # cost goes to cost_total
                            "cost_total": Sum(
                                ExpressionWrapper(F("unblended_cost") + F("markup_cost"), output_field=DecimalField())
                            )
                        },
                        "filter": [{}],
                        "cost_units_key": "currency",
                        "cost_units_fallback": "USD",
                        "sum_columns": ["cost_total", "infra_total", "sup_total"],
                        "default_ordering": {"cost_total": "desc"},
                    },
                    "tags": {"default_ordering": {"cost_total": "desc"}},
                },
                "start_date": "usage_start",
                "tables": {"query": GCPCostEntryLineItemDailySummary},
            }
        ]

        self.views = {
            "costs": {
                "default": GCPCostSummary,
                ("account",): GCPCostSummaryByAccount,
                ("region",): GCPCostSummaryByRegion,
                ("account", "region"): GCPCostSummaryByRegion,
                ("service",): GCPCostSummaryByService,
                ("account", "service"): GCPCostSummaryByService,
                ("project",): GCPCostSummaryByProject,
                ("account", "project"): GCPCostSummaryByProject,
            }
        }
        super().__init__(provider, report_type)

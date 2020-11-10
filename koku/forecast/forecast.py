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
"""Base forecasting module."""
import logging
from abc import ABC
from abc import abstractmethod
from datetime import timedelta

import statsmodels.api as sm
from statsmodels.sandbox.regression.predstd import wls_prediction_std
from statsmodels.tools.sm_exceptions import ValueWarning
from tenant_schemas.utils import tenant_context

from api.models import Provider
from api.query_filter import QueryFilterCollection
from api.report.all.openshift.provider_map import OCPAllProviderMap
from api.report.aws.openshift.provider_map import OCPAWSProviderMap
from api.report.aws.provider_map import AWSProviderMap
from api.report.azure.openshift.provider_map import OCPAzureProviderMap
from api.report.azure.provider_map import AzureProviderMap
from api.report.ocp.provider_map import OCPProviderMap
from api.utils import DateHelper


LOG = logging.getLogger(__name__)
FAKE_RESPONSE = [
    {
        "date": (DateHelper().now + timedelta(days=1)).strftime("%Y-%m-%d"),
        "value": 1,
        "confidence_min": 0,
        "confidence_max": 2,
    },
    {
        "date": (DateHelper().now + timedelta(days=2)).strftime("%Y-%m-%d"),
        "value": 2,
        "confidence_min": 1,
        "confidence_max": 3,
    },
    {
        "date": (DateHelper().now + timedelta(days=3)).strftime("%Y-%m-%d"),
        "value": 3,
        "confidence_min": 2,
        "confidence_max": 4,
    },
    {
        "date": (DateHelper().now + timedelta(days=4)).strftime("%Y-%m-%d"),
        "value": 4,
        "confidence_min": 3,
        "confidence_max": 5,
    },
    {
        "date": (DateHelper().now + timedelta(days=5)).strftime("%Y-%m-%d"),
        "value": 5,
        "confidence_min": 4,
        "confidence_max": 6,
    },
]


class Forecast(ABC):
    """Base forecasting class."""

    # the minimum number of data points needed to use the current month's data.
    # if we have fewer than this many data points, fall back to using the previous month's data.
    #
    # this number is chosen in part because statsmodels.stats.stattools.omni_normtest() needs at least eight data
    # points to test for normal distribution.
    MINIMUM = 8
    REPORT_TYPE = "costs"
    PRECISION = 8
    dh = DateHelper()

    def __init__(self, query_params):
        """Class Constructor."""
        self.params = query_params
        self.cost_summary_table = self.provider_map(self.provider, self.REPORT_TYPE).views.get("costs").get("default")

        time_scope_units = query_params.get_filter("time_scope_units", "month")
        time_scope_value = int(query_params.get_filter("time_scope_value", -1))

        if time_scope_units == "month":
            # force looking at last month if we probably won't have enough data from this month
            if self.dh.today.day <= self.MINIMUM:
                time_scope_value = -2

            if time_scope_value == -2:
                self.query_range = (self.dh.last_month_start, self.dh.today)
            else:
                self.query_range = (self.dh.this_month_start, self.dh.today)
        else:
            self.query_range = (self.dh.n_days_ago(self.dh.today, abs(time_scope_value)), self.dh.today)

        self.filters = QueryFilterCollection()
        self.filters.add(field="usage_start", operation="gte", parameter=self.query_range[0])
        self.filters.add(field="usage_end", operation="lte", parameter=self.query_range[1])

    @abstractmethod
    def predict(self):
        """Define ORM query to run forecast and return prediction."""

    def _predict(self, data):
        """Handle pre and post prediction work."""
        if len(data) < 2:
            LOG.error("Unable to calculate forecast. Insufficient Data.")
            return []

        if len(data) < self.MINIMUM:
            LOG.warning("Number of data elements is fewer than the minimum.")

        LOG.debug("Forecast input data: %s", list(data))

        # arrange the data into a form that statsmodels will accept.
        dates, costs = zip(*data)
        X = [int(d.strftime("%Y%m%d")) for d in dates]
        Y = [float(c) for c in costs]

        # run the forecast
        predicted, interval_lower, interval_upper, rsquared, pvalues = self._run_forecast(X, Y)

        response = []

        # predict() returns the same number of elements as the number of input observations
        for idx, item in enumerate(predicted):
            dikt = {
                "date": (self.dh.tomorrow + timedelta(days=idx)).strftime("%Y-%m-%d"),
                "value": round(item, self.PRECISION),
                "confidence_max": round(interval_upper[idx], self.PRECISION),
                "confidence_min": round(max(interval_lower[idx], 0), self.PRECISION),
                "rsquared": round(rsquared, self.PRECISION),
                "pvalues": round(pvalues[0], self.PRECISION),
            }
            response.append(dikt)

        return response

    def _run_forecast(self, x, y):
        """Apply the forecast model."""
        sm.add_constant(x)
        model = sm.OLS(y, x)
        results = model.fit()

        try:
            LOG.debug(results.summary())
        except (ValueWarning, UserWarning) as exc:
            LOG.warning(exc.message)

        predicted = results.predict()
        _, lower, upper = wls_prediction_std(results)

        LOG.debug("Forecast prediction: %s", predicted)
        LOG.debug("Forecast interval lower-bound: %s", lower)
        LOG.debug("Forecast interval upper-bound: %s", upper)

        return predicted, lower, upper, results.rsquared, results.pvalues.tolist()


class AWSForecast(Forecast):
    """Azure forecasting class."""

    provider = Provider.PROVIDER_AWS
    provider_map = AWSProviderMap

    def predict(self):
        """Define ORM query to run forecast and return prediction."""
        with tenant_context(self.params.tenant):
            data = (
                self.cost_summary_table.objects.filter(self.filters.compose())
                .order_by("usage_start")
                .values_list("usage_start", "unblended_cost")
            )
            return self._predict(data)


class AzureForecast(Forecast):
    """Azure forecasting class."""

    provider = Provider.PROVIDER_AZURE
    provider_map = AzureProviderMap

    def predict(self):
        """Define ORM query to run forecast and return prediction."""
        with tenant_context(self.params.tenant):
            data = (
                self.cost_summary_table.objects.filter(self.filters.compose())
                .order_by("usage_start")
                .values_list("usage_start", "pretax_cost")
            )
            return self._predict(data)


class OCPForecast(Forecast):
    """OCP forecasting class."""

    provider = Provider.PROVIDER_OCP
    provider_map = OCPProviderMap

    def predict(self):
        """Define ORM query to run forecast and return prediction."""
        with tenant_context(self.params.tenant):
            data = (
                self.cost_summary_table.objects.filter(self.filters.compose())
                .order_by("usage_start")
                .values_list("usage_start", "infrastructure_raw_cost")
            )
            return self._predict(data)


class OCPAWSForecast(Forecast):
    """OCP+AWS forecasting class."""

    provider = Provider.OCP_AWS
    provider_map = OCPAWSProviderMap

    def predict(self):
        """Define ORM query to run forecast and return prediction."""
        with tenant_context(self.params.tenant):
            data = (
                self.cost_summary_table.objects.filter(self.filters.compose())
                .order_by("usage_start")
                .values_list("usage_start", "unblended_cost")
            )
            return self._predict(data)


class OCPAzureForecast(Forecast):
    """OCP+Azure forecasting class."""

    provider = Provider.OCP_AZURE
    provider_map = OCPAzureProviderMap

    def predict(self):
        """Define ORM query to run forecast and return prediction."""
        with tenant_context(self.params.tenant):
            data = (
                self.cost_summary_table.objects.filter(self.filters.compose())
                .order_by("usage_start")
                .values_list("usage_start", "pretax_cost")
            )
            return self._predict(data)


class OCPAllForecast(Forecast):
    """OCP+All forecasting class."""

    provider = Provider.OCP_ALL
    provider_map = OCPAllProviderMap

    def predict(self):
        """Define ORM query to run forecast and return prediction."""
        with tenant_context(self.params.tenant):
            data = (
                self.cost_summary_table.objects.filter(self.filters.compose())
                .order_by("usage_start")
                .values_list("usage_start", "unblended_cost")
            )
            return self._predict(data)

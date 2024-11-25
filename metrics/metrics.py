from datetime import datetime, timezone, timedelta
import time
import requests
import pendulum
from prefect.client.schemas.objects import CsrfToken
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily, Metric

from metrics.deployments import PrefectDeployments
from metrics.flow_runs import PrefectFlowRuns
from metrics.flows import PrefectFlows
from metrics.work_pools import PrefectWorkPools
from metrics.work_queues import PrefectWorkQueues
from metrics.calculator import MetricCalculator


class PrefectMetrics(object):
    """
    PrefectMetrics class for collecting and exposing Prometheus metrics related to Prefect.
    """

    def __init__(
        self,
        url,
        headers,
        offset_minutes,
        max_retries,
        csrf_enabled,
        client_id,
        logger,
        enable_pagination,
        pagination_limit,
        collect_high_cardinality,
    ) -> None:
        """
        Initialize the PrefectMetrics instance.

        Args:
            url (str): The URL of the Prefect instance.
            headers (dict): Headers to be included in HTTP requests.
            offset_minutes (int): Time offset in minutes.
            max_retries (int): The maximum number of retries for HTTP requests.
            logger (obj): The logger object.

        """
        self.headers = headers
        self.offset_minutes = offset_minutes
        self.url = url
        self.max_retries = max_retries
        self.logger = logger
        self.client_id = client_id
        self.csrf_enabled = csrf_enabled
        self.enable_pagination = enable_pagination
        self.pagination_limit = pagination_limit
        self.csrf_token = None
        self.csrf_token_expiration = None
        self.collect_high_cardinality = collect_high_cardinality
        self.data = None
        self.calculator = None

    def collect(self) -> Metric:
        """
        Get and set Prefect work queues metrics.

        """

        ##
        # PREFECT GET CSRF TOKEN IF ENABLED
        #
        if self.csrf_enabled:
            if not self.csrf_token or pendulum.now("UTC") > self.csrf_token_expiration:
                self.logger.info(
                    "CSRF Token is expired or has not been generated yet. Fetching new CSRF Token..."
                )
                token_information = self.get_csrf_token()
                self.csrf_token = token_information.token
                self.csrf_token_expiration = token_information.expiration
            self.headers["Prefect-Csrf-Token"] = self.csrf_token
            self.headers["Prefect-Csrf-Client"] = self.client_id

        ##
        # NOTIFY IF PAGINATION IS ENABLED
        #
        if self.enable_pagination:
            self.logger.info("Pagination is enabled")
            self.logger.info(f"Pagination limit is {self.pagination_limit}")
        else:
            self.logger.info("Pagination is disabled")

        ##
        # PREFECT GET RESOURCES
        #
        self.data = self.get_current_data()
        self.calculator = MetricCalculator(self.data)

        ##
        # Calculate and output metrics prometheus client
        #
        if self.collect_high_cardinality:
            for metric in self.build_high_cardinality_metrics():
                yield metric

        for metric in self.build_low_cardinality_metrics():
            yield metric

    def get_csrf_token(self) -> CsrfToken:
        """
        Pull CSRF Token from CSRF Endpoint.

        """
        for retry in range(self.max_retries):
            try:
                csrf_token = requests.get(
                    f"{self.url}/csrf-token?client={self.client_id}"
                )
            except requests.exceptions.HTTPError as err:
                self.logger.error(err)
                if retry >= self.max_retries - 1:
                    time.sleep(1)
                    raise SystemExit(err)
            else:
                break
        return CsrfToken.parse_obj(csrf_token.json())

    def build_high_cardinality_metrics(self) -> Metric:
        """
        A method to gather high cardinality metrics if the environment allows
        """

        prefect_info_deployments = GaugeMetricFamily(
            "prefect_info_deployment",
            "Prefect deployment info",
            labels=[
                "created",
                "flow_id",
                "flow_name",
                "deployment_id",
                "is_schedule_active",
                "deployment_name",
                "path",
                "paused",
                "work_pool_name",
                "work_queue_name",
                "status",
            ],
        )
        self.calculator.calculate_prefect_info_deployments(
            metric=prefect_info_deployments
        )
        yield prefect_info_deployments

        prefect_info_flows = GaugeMetricFamily(
            "prefect_info_flows",
            "Prefect flow info",
            labels=["created", "flow_id", "flow_name"],
        )
        self.calculator.calculate_prefect_info_flows(metric=prefect_info_flows)
        yield prefect_info_flows

        prefect_flow_runs_total_run_time = CounterMetricFamily(
            "prefect_flow_runs_total_run_time",
            "Prefect flow-run total run time in seconds",
            labels=["flow_id", "flow_name", "flow_run_name"],
        )
        self.calculator.calculate_prefect_flow_runs_total_run_time(
            metric=prefect_flow_runs_total_run_time
        )
        yield prefect_flow_runs_total_run_time

        prefect_info_flow_runs = GaugeMetricFamily(
            "prefect_info_flow_runs",
            "Prefect flow runs info",
            labels=[
                "created",
                "deployment_id",
                "deployment_name",
                "end_time",
                "flow_id",
                "flow_name",
                "flow_run_id",
                "flow_run_name",
                "run_count",
                "start_time",
                "state_id",
                "state_name",
                "total_run_time",
                "work_queue_name",
            ],
        )
        self.calculator.calculate_prefect_info_flow_runs(metric=prefect_info_flow_runs)
        yield prefect_info_flow_runs

        prefect_info_work_pools = GaugeMetricFamily(
            "prefect_info_work_pools",
            "Prefect work pools info",
            labels=[
                "created",
                "work_queue_id",
                "work_pool_id",
                "is_paused",
                "work_pool_name",
                "type",
                "status",
            ],
        )
        self.calculator.calculate_prefect_info_work_pools(
            metric=prefect_info_work_pools
        )
        yield prefect_info_work_pools

        prefect_info_work_queues = GaugeMetricFamily(
            "prefect_info_work_queues",
            "Prefect work queues info",
            labels=[
                "created",
                "work_queue_id",
                "is_paused",
                "work_queue_name",
                "priority",
                "type",
                "work_pool_id",
                "work_pool_name",
                "status",
                "healthy",
                "late_runs_count",
                "last_polled",
                "health_check_policy_maximum_late_runs",
                "health_check_policy_maximum_seconds_since_last_polled",
            ],
        )
        self.calculator.calculate_prefect_info_work_queues(
            metric=prefect_info_work_queues
        )
        yield prefect_info_work_queues

    def build_low_cardinality_metrics(self) -> Metric:
        """
        A method to gather low cardinality metrics
        """

        prefect_deployments = GaugeMetricFamily(
            "prefect_deployments_total", "Prefect total deployments", labels=[]
        )
        self.calculator.calculate_prefect_deployments_total(metric=prefect_deployments)
        yield prefect_deployments

        prefect_flows = GaugeMetricFamily(
            "prefect_flows_total", "Prefect total flows", labels=[]
        )
        self.calculator.calculate_prefect_flows_total(metric=prefect_flows)
        yield prefect_flows

        prefect_flow_runs = GaugeMetricFamily(
            "prefect_flow_runs_total", "Prefect total flow runs", labels=[]
        )
        self.calculator.calculate_prefect_flow_runs_total(metric=prefect_flow_runs)
        yield prefect_flow_runs

        prefect_work_pools = GaugeMetricFamily(
            "prefect_work_pools_total", "Prefect total work pools", labels=[]
        )
        self.calculator.calculate_prefect_work_pools_total(metric=prefect_work_pools)
        yield prefect_work_pools

        prefect_work_queues = GaugeMetricFamily(
            "prefect_work_queues_total", "Prefect total work queues", labels=[]
        )
        self.calculator.calculate_prefect_work_queues_total(metric=prefect_work_queues)
        yield prefect_work_queues

        prefect_flow_run_state = GaugeMetricFamily(
            "prefect_flow_run_state_total",
            "Aggregate state metrics for prefect flow runs",
            labels=["state"],
        )
        self.calculator.calculate_flow_run_state_metrics(metric=prefect_flow_run_state)
        yield prefect_flow_run_state

        prefect_flow_run_state_past_24_hours = GaugeMetricFamily(
            "prefect_flow_run_state_past_24_hours_total",
            "Aggregate state metrics for prefect flow runs timestamped in the past 24 hours",
            labels=["state"],
        )
        start_24_hour_period_timestamp = datetime.now(timezone.utc) - timedelta(
            hours=24
        )
        self.calculator.calculate_flow_run_state_metrics(
            metric=prefect_flow_run_state_past_24_hours,
            start_timestamp=start_24_hour_period_timestamp,
        )
        yield prefect_flow_run_state_past_24_hours

    def get_current_data(self) -> {str, any}:
        """
        Gathers the data requried to calcualte metrics and returns it in a single dictonary object.
        """
        current_data = dict()

        current_data["deployments"] = PrefectDeployments(
            self.url,
            self.headers,
            self.max_retries,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_deployments_info()

        current_data["flows"] = PrefectFlows(
            self.url,
            self.headers,
            self.max_retries,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_flows_info()

        current_data["flow_runs"] = PrefectFlowRuns(
            self.url,
            self.headers,
            self.max_retries,
            self.offset_minutes,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_flow_runs_info()

        current_data["all_flow_runs"] = PrefectFlowRuns(
            self.url,
            self.headers,
            self.max_retries,
            self.offset_minutes,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_all_flow_runs_info()

        current_data["work_pools"] = PrefectWorkPools(
            self.url,
            self.headers,
            self.max_retries,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_work_pools_info()

        current_data["work_queues"] = PrefectWorkQueues(
            self.url,
            self.headers,
            self.max_retries,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_work_queues_info()

        return current_data

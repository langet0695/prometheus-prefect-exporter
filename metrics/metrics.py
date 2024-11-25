from metrics.deployments import PrefectDeployments
from metrics.flow_runs import PrefectFlowRuns
from metrics.flows import PrefectFlows
from metrics.work_pools import PrefectWorkPools
from metrics.work_queues import PrefectWorkQueues
import requests
import time
from prefect.client.schemas.objects import CsrfToken
import pendulum
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily
from datetime import datetime, timezone, timedelta


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

    def collect(self):
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
        # PREFECT GET RESOURCES
        #
        self.data = self.get_current_data()

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

    def build_high_cardinality_metrics(self):
        """
        A method to gather high cardinality metrics if the environment allows
        """

        # pull required objects out of the data object
        deployments = self.data.get("deployments")
        flows = self.data.get("flows")
        flow_runs = self.data.get("flow_runs")
        all_flow_runs = self.data.get("all_flow_runs")
        work_pools = self.data.get("work_pools")
        work_queues = self.data.get("work_queues")

        ##
        # PREFECT DEPLOYMENTS METRICS
        #

        prefect_deployments = GaugeMetricFamily(
            "prefect_deployments_total", "Prefect total deployments", labels=[]
        )
        prefect_deployments.add_metric([], len(deployments))
        yield prefect_deployments

        # prefect_info_deployments metric
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

        for deployment in deployments:
            # get flow name
            if deployment.get("flow_id") is None:
                flow_name = "null"
            else:
                flow_name = next(
                    (
                        flow.get("name")
                        for flow in flows
                        if flow.get("id") == deployment.get("flow_id")
                    ),
                    "null",
                )

            # The "is_schedule_active" field is deprecated, and always returns
            # "null". For backward compatibility, we will populate the value of
            # this label with the "paused" field.
            is_schedule_active = deployment.get("paused", "null")
            if is_schedule_active != "null":
                # Negate the value we get from "paused" because "is_schedule_active"
                # is the opposite of "paused".
                is_schedule_active = not is_schedule_active

            prefect_info_deployments.add_metric(
                [
                    str(deployment.get("created", "null")),
                    str(deployment.get("flow_id", "null")),
                    str(flow_name),
                    str(deployment.get("id", "null")),
                    str(is_schedule_active),
                    str(deployment.get("name", "null")),
                    str(deployment.get("path", "null")),
                    str(deployment.get("paused", "null")),
                    str(deployment.get("work_pool_name", "null")),
                    str(deployment.get("work_queue_name", "null")),
                    str(deployment.get("status", "null")),
                ],
                1,
            )

        yield prefect_info_deployments

        ##
        # PREFECT FLOWS METRICS
        #

        # prefect_flows metric
        prefect_flows = GaugeMetricFamily(
            "prefect_flows_total", "Prefect total flows", labels=[]
        )
        prefect_flows.add_metric([], len(flows))
        yield prefect_flows

        # prefect_info_flows metric
        prefect_info_flows = GaugeMetricFamily(
            "prefect_info_flows",
            "Prefect flow info",
            labels=["created", "flow_id", "flow_name"],
        )

        for flow in flows:
            prefect_info_flows.add_metric(
                [
                    str(flow.get("created", "null")),
                    str(flow.get("id", "null")),
                    str(flow.get("name", "null")),
                ],
                1,
            )

        yield prefect_info_flows

        ##
        # PREFECT FLOW RUNS METRICS
        #

        # prefect_flow_runs metric
        prefect_flow_runs = GaugeMetricFamily(
            "prefect_flow_runs_total", "Prefect total flow runs", labels=[]
        )
        prefect_flow_runs.add_metric([], len(all_flow_runs))
        yield prefect_flow_runs

        # prefect_flow_runs_total_run_time metric
        prefect_flow_runs_total_run_time = CounterMetricFamily(
            "prefect_flow_runs_total_run_time",
            "Prefect flow-run total run time in seconds",
            labels=["flow_id", "flow_name", "flow_run_name"],
        )

        for flow_run in all_flow_runs:
            # get deployment name
            if flow_run.get("deployment_id") is None:
                deployment_name = "null"
            else:
                deployment_name = next(
                    (
                        deployment.get("name")
                        for deployment in deployments
                        if flow_run.get("deployment_id") == deployment.get("id")
                    ),
                    "null",
                )

            # get flow name
            if flow_run.get("flow_id") is None:
                flow_name = "null"
            else:
                flow_name = next(
                    (
                        flow.get("name")
                        for flow in flows
                        if flow.get("id") == flow_run.get("flow_id")
                    ),
                    "null",
                )

            prefect_flow_runs_total_run_time.add_metric(
                [
                    str(flow_run.get("flow_id", "null")),
                    str(flow_name),
                    str(flow_run.get("name", "null")),
                ],
                str(flow_run.get("total_run_time", "null")),
            )

        yield prefect_flow_runs_total_run_time

        # prefect_info_flow_runs metric
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

        for flow_run in flow_runs:
            # get deployment name
            if flow_run.get("deployment_id") is None:
                deployment_name = "null"
            else:
                deployment_name = next(
                    (
                        deployment.get("name")
                        for deployment in deployments
                        if flow_run.get("deployment_id") == deployment.get("id")
                    ),
                    "null",
                )

            # get flow name
            if flow_run.get("flow_id") is None:
                flow_name = "null"
            else:
                flow_name = next(
                    (
                        flow.get("name")
                        for flow in flows
                        if flow.get("id") == flow_run.get("flow_id")
                    ),
                    "null",
                )

            # set state
            state = 0 if flow_run.get("state_name") != "Running" else 1
            prefect_info_flow_runs.add_metric(
                [
                    str(flow_run.get("created", "null")),
                    str(flow_run.get("deployment_id", "null")),
                    str(deployment_name),
                    str(flow_run.get("end_time", "null")),
                    str(flow_run.get("flow_id", "null")),
                    str(flow_name),
                    str(flow_run.get("id", "null")),
                    str(flow_run.get("name", "null")),
                    str(flow_run.get("run_count", "null")),
                    str(flow_run.get("start_time", "null")),
                    str(flow_run.get("state_id", "null")),
                    str(flow_run.get("state_name", "null")),
                    str(flow_run.get("total_run_time", "null")),
                    str(flow_run.get("work_queue_name", "null")),
                ],
                state,
            )

        yield prefect_info_flow_runs

        ##
        # PREFECT WORK POOLS METRICS
        #

        # prefect_work_pools metric
        prefect_work_pools = GaugeMetricFamily(
            "prefect_work_pools_total", "Prefect total work pools", labels=[]
        )
        prefect_work_pools.add_metric([], len(work_pools))
        yield prefect_work_pools

        # prefect_info_work_pools metric
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

        for work_pool in work_pools:
            state = 0 if work_pool.get("is_paused") else 1
            prefect_info_work_pools.add_metric(
                [
                    str(work_pool.get("created", "null")),
                    str(work_pool.get("default_queue_id", "null")),
                    str(work_pool.get("id", "null")),
                    str(work_pool.get("is_paused", "null")),
                    str(work_pool.get("name", "null")),
                    str(work_pool.get("type", "null")),
                    str(work_pool.get("status", "null")),
                ],
                state,
            )

        yield prefect_info_work_pools

        ##
        # PREFECT WORK QUEUES METRICS
        #

        # prefect_work_queues metric
        prefect_work_queues = GaugeMetricFamily(
            "prefect_work_queues_total", "Prefect total work queues", labels=[]
        )
        prefect_work_queues.add_metric([], len(work_queues))
        yield prefect_work_queues

        # prefect_info_work_queues metric
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

        for work_queue in work_queues:
            state = 0 if work_queue.get("is_paused") else 1
            status_info = work_queue.get("status_info", {})
            health_check_policy = status_info.get("health_check_policy", {})
            prefect_info_work_queues.add_metric(
                [
                    str(work_queue.get("created", "null")),
                    str(work_queue.get("id", "null")),
                    str(work_queue.get("is_paused", "null")),
                    str(work_queue.get("name", "null")),
                    str(work_queue.get("priority", "null")),
                    str(work_queue.get("type", "null")),
                    str(work_queue.get("work_pool_id", "null")),
                    str(work_queue.get("work_pool_name", "null")),
                    str(work_queue.get("status", "null")),
                    str(status_info.get("healthy", "null")),
                    str(status_info.get("late_runs_count", "null")),
                    str(status_info.get("last_polled", "null")),
                    str(health_check_policy.get("maximum_late_runs", "null")),
                    str(
                        health_check_policy.get(
                            "maximum_seconds_since_last_polled", "null"
                        )
                    ),
                ],
                state,
            )

        yield prefect_info_work_queues

    def build_low_cardinality_metrics(self):
        """
        A method to gather low cardinality metrics
        """

        prefect_flow_run_state_total = GaugeMetricFamily(
            "prefect_flow_run_state_total",
            "Aggregate state metrics for prefect flow runs",
            labels=["state"],
        )
        self.calculate_flow_run_state_metrics(metric=prefect_flow_run_state_total)
        yield prefect_flow_run_state_total

        prefect_flow_run_state_past_24_hours_total = GaugeMetricFamily(
            "prefect_flow_run_state_past_24_hours_total",
            "Aggregate state metrics for prefect flow runs timestamped in the past 24 hours",
            labels=["state"],
        )
        start_24_hour_period_timestamp = datetime.now(timezone.utc) - timedelta(
            hours=24
        )
        self.calculate_flow_run_state_metrics(
            metric=prefect_flow_run_state_past_24_hours_total,
            start_timestamp=start_24_hour_period_timestamp,
        )
        yield prefect_flow_run_state_past_24_hours_total

    def calculate_flow_run_state_metrics(
        self,
        metric,
        start_timestamp=datetime(1970, 1, 1, tzinfo=timezone.utc),
        end_timestamp=datetime.now(timezone.utc),
    ) -> any:
        # pull required objects out of the data object
        all_flow_runs = self.data.get("all_flow_runs")

        state_total = {
            "Failed": 0,
            "Crashed": 0,
            "Running": 0,
            "Cancelled": 0,
            "Completed": 0,
            "Pending": 0,
            "Scheduled": 0,
            "Late": 0,
        }

        for flow_run in all_flow_runs:
            state = flow_run["state"]["name"]
            timestamp = datetime.strptime(
                flow_run["state"]["timestamp"], "%Y-%m-%dT%H:%M:%S.%f%z"
            )

            if state_total.get(state, None) is None:
                state_total.update({state: 0})

            if start_timestamp <= timestamp <= end_timestamp:
                state_total[state] += 1

        for state, total in state_total.items():
            metric.add_metric([state], total)

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

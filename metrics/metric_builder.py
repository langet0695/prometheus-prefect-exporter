from datetime import datetime, timezone, timedelta
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily, Metric
from metrics.calculator import MetricCalculator


class MetricBuilder:
    """
    A class used to build prometheus metrics as instructed by the system
    """

    def __init__(self, data) -> None:
        self.calculator = MetricCalculator(data)
        self.mapping_dict = self.get_mapping_dict()

    def get_mapping_dict(self) -> {str, callable}:
        """
        Returns a static dictionary of metrics that can be built and the associated method to build them.

        Args:
            None
        """
        mapping_dict = {
            "prefect_deployments_total": self.build_prefect_deployments_total,
            "prefect_info_deployments": self.build_prefect_info_deployments,
            "prefect_flows_total": self.build_prefect_flows_total,
            "prefect_info_flows": self.build_prefect_info_flows,
            "prefect_flow_runs_total": self.build_prefect_flow_runs_total,
            "prefect_flow_runs_total_run_time": self.build_prefect_flow_runs_total_run_time,
            "prefect_info_flow_runs": self.build_prefect_info_flow_runs,
            "prefect_work_pools_total": self.build_prefect_work_pools_total,
            "prefect_info_work_pools": self.build_prefect_info_work_pools,
            "prefect_work_queues_total": self.build_prefect_work_queues_total,
            "prefect_info_work_queues": self.build_prefect_info_work_queues,
            "prefect_flow_run_state_total": self.build_prefect_flow_run_state_total,
            "prefect_flow_run_state_past_24_hours": self.build_prefect_flow_run_state_past_24_hours,
        }
        return mapping_dict

    def get_builder_method(self, method_name: str) -> callable:
        """
        Returns a method that will build a metric based on the metric requested.

        Args:
            method_name(String): A string that will be used as a key to return the relevant method
        """
        return self.mapping_dict.get(method_name)

    def build_prefect_deployments_total(self) -> Metric:
        """
        Build the total deployments metric
        """
        prefect_deployments = GaugeMetricFamily(
            "prefect_deployments_total", "Prefect total deployments", labels=[]
        )
        self.calculator.calculate_prefect_deployments_total(metric=prefect_deployments)
        yield prefect_deployments

    def build_prefect_info_deployments(self) -> Metric:
        """
        Build a high cardinality info deployments metric
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

    def build_prefect_flows_total(self) -> Metric:
        """
        Build the total flows metric
        """
        prefect_flows = GaugeMetricFamily(
            "prefect_flows_total", "Prefect total flows", labels=[]
        )
        self.calculator.calculate_prefect_flows_total(metric=prefect_flows)
        yield prefect_flows

    def build_prefect_info_flows(self) -> Metric:
        """
        Build a high cardinality info flows metric
        """
        prefect_info_flows = GaugeMetricFamily(
            "prefect_info_flows",
            "Prefect flow info",
            labels=["created", "flow_id", "flow_name"],
        )
        self.calculator.calculate_prefect_info_flows(metric=prefect_info_flows)
        yield prefect_info_flows

    def build_prefect_flow_runs_total(self) -> Metric:
        """
        Report on total flow runs
        """
        prefect_flow_runs = GaugeMetricFamily(
            "prefect_flow_runs_total", "Prefect total flow runs", labels=[]
        )
        self.calculator.calculate_prefect_flow_runs_total(metric=prefect_flow_runs)
        yield prefect_flow_runs

    def build_prefect_flow_runs_total_run_time(self) -> Metric:
        """
        Total the flow run time
        """
        prefect_flow_runs_total_run_time = CounterMetricFamily(
            "prefect_flow_runs_total_run_time",
            "Prefect flow-run total run time in seconds",
            labels=["flow_id", "flow_name", "flow_run_name"],
        )
        self.calculator.calculate_prefect_flow_runs_total_run_time(
            metric=prefect_flow_runs_total_run_time
        )
        yield prefect_flow_runs_total_run_time

    def build_prefect_info_flow_runs(self) -> Metric:
        """
        Build a high cardinality metric displaying info on flow runs
        """
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

    def build_prefect_work_pools_total(self) -> Metric:
        """
        Build a metric totaling work pools
        """
        prefect_work_pools = GaugeMetricFamily(
            "prefect_work_pools_total", "Prefect total work pools", labels=[]
        )
        self.calculator.calculate_prefect_work_pools_total(metric=prefect_work_pools)
        yield prefect_work_pools

    def build_prefect_info_work_pools(self) -> Metric:
        """
        Build a high cardinality metric displaying info on work pools
        """
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

    def build_prefect_work_queues_total(self) -> Metric:
        """
        Build a metric totaling all work queues
        """
        prefect_work_queues = GaugeMetricFamily(
            "prefect_work_queues_total", "Prefect total work queues", labels=[]
        )
        self.calculator.calculate_prefect_work_queues_total(metric=prefect_work_queues)
        yield prefect_work_queues

    def build_prefect_info_work_queues(self) -> Metric:
        """
        Build a high cardinality metric displaying info on work queues
        """
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

    def build_prefect_flow_run_state_total(self) -> Metric:
        """
        Build a metric aggregating all flow run states
        """
        prefect_flow_run_state = GaugeMetricFamily(
            "prefect_flow_run_state_total",
            "Aggregate state metrics for prefect flow runs",
            labels=["state"],
        )
        self.calculator.calculate_flow_run_state_metrics(metric=prefect_flow_run_state)
        yield prefect_flow_run_state

    def build_prefect_flow_run_state_past_24_hours(self) -> Metric:
        """
        Build a metric aggregating prefect flow states from the past 24 hours
        """
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

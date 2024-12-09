from datetime import datetime, timezone
from prometheus_client.core import Metric


class MetricCalculator:
    """
    A class used to calculate and add Prometheus metrics from Prefect data.
    """

    def __init__(self, data: dict) -> None:
        """
        Initialize an instance of the MetricCalculator.

        Args:
            data (dict): A dictonary of objects containing data collected from Prefect
        """
        self.data = data

    def calculate_flow_run_state_metrics(
        self,
        metric: Metric,
        start_timestamp: datetime = datetime(1970, 1, 1, tzinfo=timezone.utc),
        end_timestamp: datetime = datetime.now(timezone.utc),
        source="all_flow_runs",
    ) -> None:
        """
        Aggregation of flow runs segmented by state.

        Args:
            metric (Metric): A prometheus metric object that will have the calculated metric applied
            start_timestamp(Datetime): Represents the start of the flow run timestamps that will be included
            end_timestamp(Datetime): Represents the end of the flow run timestamps that will be included
        """

        flow_runs = self.data.get(source)

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

        for flow_run in flow_runs:
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

    def calculate_prefect_deployments_total(self, metric: Metric) -> None:
        """
        Aggregate calculation of prefect deployments and apply to the provided metric object
        """

        deployments = self.data.get("deployments")

        metric.add_metric([], len(deployments))

    def calculate_prefect_info_deployments(self, metric: Metric) -> None:
        """
        Calculate metadata by deployment and apply to given metric object
        """

        deployments = self.data.get("deployments")
        flows = self.data.get("flows")

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

            metric.add_metric(
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

    def calculate_prefect_flows_total(self, metric: Metric) -> None:
        """
        Aggregate calculation of prefect flows and apply to the provided metric object
        """

        flows = self.data.get("flows")

        metric.add_metric([], len(flows))

    def calculate_prefect_info_flows(self, metric: Metric) -> None:
        """
        Calculate metadata by flow and apply to given metric object
        """

        flows = self.data.get("flows")

        for flow in flows:
            metric.add_metric(
                [
                    str(flow.get("created", "null")),
                    str(flow.get("id", "null")),
                    str(flow.get("name", "null")),
                ],
                1,
            )

    def calculate_prefect_flow_runs_total(self, metric: Metric) -> None:
        """
        Aggregate calculation of prefect flow runs and apply to the provided metric object
        """

        all_flow_runs = self.data.get("all_flow_runs")

        metric.add_metric([], len(all_flow_runs))

    def calculate_prefect_flow_runs_total_run_time(self, metric: Metric) -> None:
        """
        Calculate total run time by flow and apply to given metric object
        """

        all_flow_runs = self.data.get("all_flow_runs")
        flows = self.data.get("flows")

        for flow_run in all_flow_runs:
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

            metric.add_metric(
                [
                    str(flow_run.get("flow_id", "null")),
                    str(flow_name),
                    str(flow_run.get("name", "null")),
                ],
                str(flow_run.get("total_run_time", "null")),
            )

    def calculate_prefect_info_flow_runs(self, metric: Metric) -> None:
        """
        Calculate metadata by flow run and apply to given metric object
        """

        deployments = self.data.get("deployments")
        flows = self.data.get("flows")
        flow_runs = self.data.get("flow_runs")

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
            metric.add_metric(
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

    def calculate_prefect_work_pools_total(self, metric: Metric) -> None:
        """
        Aggregate calculation of work pools and apply to the provided metric object
        """

        work_pools = self.data.get("work_pools")

        metric.add_metric([], len(work_pools))

    def calculate_prefect_info_work_pools(self, metric: Metric) -> None:
        """
        Calculate metadata by work pool and apply to given metric object
        """

        work_pools = self.data.get("work_pools")

        for work_pool in work_pools:
            state = 0 if work_pool.get("is_paused") else 1
            metric.add_metric(
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

    def calculate_prefect_work_queues_total(self, metric: Metric) -> None:
        """
        Aggregate calculation of work queues and apply to the provided metric object
        """

        work_queues = self.data.get("work_queues")

        metric.add_metric([], len(work_queues))

    def calculate_prefect_info_work_queues(self, metric: Metric) -> None:
        """
        Calculate metadata by work queue and apply to given metric object
        """

        work_queues = self.data.get("work_queues")

        for work_queue in work_queues:
            state = 0 if work_queue.get("is_paused") else 1
            status_info = work_queue.get("status_info", {})
            health_check_policy = status_info.get("health_check_policy", {})
            metric.add_metric(
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

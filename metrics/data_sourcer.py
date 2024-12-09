from metrics.deployments import PrefectDeployments
from metrics.flow_runs import PrefectFlowRuns
from metrics.flows import PrefectFlows
from metrics.work_pools import PrefectWorkPools
from metrics.work_queues import PrefectWorkQueues


class DataSourcer:
    """
    A class used to source data that can be used to build prometheus metrics.
    """

    def __init__(
        self,
        url: str,
        headers,
        max_retries: int,
        logger: object,
        enable_pagination: bool,
        pagination_limit: int,
        offset_minutes,
    ):
        self.url = url
        self.headers = headers
        self.max_retries = max_retries
        self.logger = logger
        self.enable_pagination = enable_pagination
        self.pagination_limit = pagination_limit
        self.offset_minutes = offset_minutes
        self.mapping_dict = self.get_mapping_dict()

    def get_mapping_dict(self) -> {str, callable}:
        """
        Returns a static dictionary of prefect data sources that can be gathered and the associated method to retrieve them.

        Args:
            None
        """
        mapping_dict = {
            "deployments": self.get_deployments,
            "flows": self.get_flows,
            "flow_runs": self.get_flow_runs,
            "all_flow_runs": self.get_all_flow_runs,
            "work_pools": self.get_work_pools,
            "work_queues": self.get_work_queues,
        }
        return mapping_dict

    def get_sourcing_method(self, method_name: str) -> callable:
        """
        Returns a method that will gather source data based on the data requested.

        Args:
            method_name(String): A string that will be used as a key to return the relevant method
        """
        return self.mapping_dict.get(method_name)

    def get_deployments(self):
        """
        Fetch deployment data
        """
        deployments = PrefectDeployments(
            self.url,
            self.headers,
            self.max_retries,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_deployments_info()
        return deployments

    def get_flows(self):
        """
        Fetch flow data
        """
        flows = PrefectFlows(
            self.url,
            self.headers,
            self.max_retries,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_flows_info()
        return flows

    def get_flow_runs(self):
        """
        Fetch flow run data in the legacy manner
        """
        flow_runs = PrefectFlowRuns(
            self.url,
            self.headers,
            self.max_retries,
            self.offset_minutes,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_flow_runs_info()
        return flow_runs

    def get_all_flow_runs(self):
        """
        Fetch all flow run from the entire history of the given prefect instance
        """
        all_flow_runs = PrefectFlowRuns(
            self.url,
            self.headers,
            self.max_retries,
            self.offset_minutes,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_all_flow_runs_info()
        return all_flow_runs

    def get_work_pools(self):
        """
        Fetch work pool data
        """
        work_pools = PrefectWorkPools(
            self.url,
            self.headers,
            self.max_retries,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_work_pools_info()
        return work_pools

    def get_work_queues(self):
        """
        Fetch work queue data
        """
        work_queues = PrefectWorkQueues(
            self.url,
            self.headers,
            self.max_retries,
            self.logger,
            self.enable_pagination,
            self.pagination_limit,
        ).get_work_queues_info()
        return work_queues

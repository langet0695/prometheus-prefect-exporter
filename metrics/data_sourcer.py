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
        headers: dict,
        max_retries: int,
        logger: object,
        enable_pagination: bool,
        pagination_limit: int,
        offset_minutes: int,
    ):
        self.url = url
        self.headers = headers
        self.max_retries = max_retries
        self.logger = logger
        self.enable_pagination = enable_pagination
        self.pagination_limit = pagination_limit
        self.offset_minutes = offset_minutes

    def fetch_data(self, source: str) -> callable:
        """
        Gathers source data based on the source requested.

        Args:
            source (str): The source to fetch data from
        """

        match source:
            case "deployments":
                return self.get_deployments()
            case "flows":
                return self.get_flows()
            case "flow_runs":
                return self.get_flow_runs()
            case "all_flow_runs":
                return self.get_all_flow_runs()
            case "work_pools":
                return self.get_work_pools()
            case "work_queues":
                return self.get_work_queues()
            case _:
                raise ValueError("Unknown source: " + source)

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

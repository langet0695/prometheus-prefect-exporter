import json
import time
import requests
import pendulum
from prefect.client.schemas.objects import CsrfToken
from prometheus_client.core import Metric


from metrics.data_sourcer import DataSourcer
from metrics.metric_builder import MetricBuilder


class PrefectMetrics(object):
    """
    PrefectMetrics class for collecting and exposing Prometheus metrics related to Prefect.
    """

    def __init__(
        self,
        url: str,
        headers,
        offset_minutes,
        max_retries: int,
        csrf_enabled: bool,
        client_id: str,
        logger: object,
        enable_pagination: bool,
        pagination_limit: int,
        target_metrics: list,
    ) -> None:
        """
        Initialize the PrefectMetrics instance.

        Args:
            url (str): The URL of the Prefect instance.
            headers (dict): Headers to be included in HTTP requests.
            offset_minutes (int): Time offset in minutes.
            max_retries (int): The maximum number of retries for HTTP requests.
            logger (obj): The logger object.
            enable_pagination (bool): Indicates if pagination is enbabled.
            pagination_limit (int): How many records per page.
            target_metrics(list): A list of metrics or metric groups that should be collected
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
        self.target_metrics = target_metrics
        self.data = {}
        self.calculator = None

        ##
        # Get lists data sources to fetch and metrics to load
        #
        (
            self.metrics_to_collect,
            self.data_sources,
        ) = self.get_data_calculation_mappings()
        self.logger.info("Metrics to Collect: %s", str(self.metrics_to_collect))
        self.logger.info("Data Sources: %s", str(self.data_sources))

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
        # Get Prefect resources based on required data sources
        #
        sourcer = DataSourcer(
            url=self.url,
            headers=self.headers,
            max_retries=self.max_retries,
            logger=self.logger,
            enable_pagination=self.enable_pagination,
            pagination_limit=self.pagination_limit,
            offset_minutes=self.offset_minutes,
        )
        for source in self.data_sources:
            start = time.time()
            fetch_source_data = sourcer.get_sourcing_method(source)
            self.logger.info("%s fetch in second: %d", source, (time.time() - start))
            self.data[source] = fetch_source_data()

        ##
        # Build and output metrics to the prometheus client
        #
        builder = MetricBuilder(
            data=self.data,
            offset_minutes=self.offset_minutes,
        )
        for metric in self.metrics_to_collect:
            build_metrics_output = builder.get_builder_method(metric)
            for metric in build_metrics_output():
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

    def get_data_calculation_mappings(self):
        """
        Builds two sets that represent all the metrics to calculate and data sources required based on what the environment dictates is required.
        """
        with open(
            "metrics/data_calculation_mappings.json", "r", encoding="utf-8"
        ) as file:
            mapping_data = json.load(file)
        metrics_to_collect = set()
        data_sources = set()
        for target in self.target_metrics:
            for metric in mapping_data[target]["metrics_to_collect"]:
                metrics_to_collect.add(metric)
            for data_source in mapping_data[target]["data_sources_needed"]:
                data_sources.add(data_source)
        return metrics_to_collect, data_sources

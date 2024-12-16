import os
import logging
import time
import uuid
import json

from dotenv import load_dotenv
from prometheus_client import start_http_server, REGISTRY

from utils.auth import Authorization

from metrics.metrics import PrefectMetrics
from metrics.healthz import PrefectHealthz


def metrics():
    """
    Main entry point for the PrefectMetrics exporter.
    """

    # Get environment variables or use default values
    load_dotenv()
    loglevel = str(os.getenv("LOG_LEVEL", "INFO"))
    max_retries = int(os.getenv("MAX_RETRIES", "3"))
    metrics_port = int(os.getenv("METRICS_PORT", "8000"))
    offset_minutes = int(os.getenv("OFFSET_MINUTES", "5"))
    url = str(os.getenv("PREFECT_API_URL", "http://localhost:4200/api"))
    api_key = str(os.getenv("PREFECT_API_KEY", ""))
    api_user = str(os.getenv("PREFECT_API_USER", ""))
    api_password = str(os.getenv("PREFECT_API_PASSWORD", ""))
    enable_pagination = str(os.getenv("PAGINATION_ENABLED", "True")) == "True"
    pagination_limit = int(os.getenv("PAGINATION_LIMIT", 200))
    csrf_enabled = str(os.getenv("PREFECT_CSRF_ENABLED", "False")) == "True"
    target_metrics = json.loads(os.getenv("TARGET_METRICS", '["legacy_metrics"]'))

    csrf_client_id = str(uuid.uuid4())
    # Configure logging
    logging.basicConfig(
        level=loglevel, format="%(asctime)s - %(name)s - [%(levelname)s] %(message)s"
    )
    logger = logging.getLogger("prometheus-prefect-exporter")

    # Configure headers for HTTP requests
    headers = {"accept": "application/json", "Content-Type": "application/json"}

    auth_value = Authorization(
        api_key=api_key,
        api_user=api_user,
        api_password=api_password,
        logger=logger,
    ).get_auth_header()
    headers["Authorization"] = auth_value

    # check endpoint
    PrefectHealthz(
        url=url, headers=headers, max_retries=max_retries, logger=logger
    ).get_health_check()

    ##
    # NOTIFY IF PAGINATION IS ENABLED
    #
    if enable_pagination:
        logger.info("Pagination is enabled")
        logger.info(f"Pagination limit is {pagination_limit}")
    else:
        logger.info("Pagination is disabled")

    # Create an instance of the PrefectMetrics class
    metrics = PrefectMetrics(
        url=url,
        headers=headers,
        offset_minutes=offset_minutes,
        max_retries=max_retries,
        client_id=csrf_client_id,
        csrf_enabled=csrf_enabled,
        logger=logger,
        # Enable pagination if not specified to avoid breaking existing deployments
        enable_pagination=enable_pagination,
        pagination_limit=pagination_limit,
        target_metrics=target_metrics,
    )

    # Register the metrics with Prometheus
    logger.info("Initializing metrics...")
    REGISTRY.register(metrics)

    # Start the HTTP server to expose Prometheus metrics
    start_http_server(metrics_port)
    logger.info("Exporter listening on port :%i", metrics_port)

    # Run the loop to collect Prefect metrics
    while True:
        time.sleep(5)


if __name__ == "__main__":
    metrics()

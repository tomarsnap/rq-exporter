"""
RQ metrics collector.

"""

import logging

from rq import Connection, Worker
from prometheus_client import Summary
from prometheus_client.core import GaugeMetricFamily

from .utils import get_jobs_by_queue, clean_all_registries, count_all_workers

logger = logging.getLogger(__name__)


class RQCollector(object):
    """RQ stats collector.

    Args:
        connection (redis.Redis): Redis connection instance.
        worker_class (type): RQ Worker class
        queue_class (type): RQ Queue class

    """

    def __init__(self, connection=None, worker_class=None, queue_class=None):
        self.connection = connection
        self.worker_class = worker_class
        self.queue_class = queue_class

        # RQ data collection count and time in seconds
        self.summary = Summary('rq_request_processing_seconds', 'Time spent collecting RQ data')

    def collect(self):
        """Collect RQ Metrics.

        Note:
            This method will be called on registration and every time the metrics are requested.

        Yields:
            RQ metrics for workers and jobs.

        Raises:
            redis.exceptions.RedisError: On Redis connection errors

        """
        logger.debug('Collecting the RQ metrics...')

        with self.summary.time():
            with Connection(self.connection):
                rq_workers = GaugeMetricFamily('rq_workers_count', 'Number of RQ workers')
                rq_jobs = GaugeMetricFamily('rq_jobs', 'RQ jobs by state', labels=['queue', 'status'])

                clean_all_registries(self.queue_class)
                workers = count_all_workers(self.connection, self.worker_class)
                rq_workers.add_metric([], workers)
                # Add same label to started and queued statuses if combining doesn't work in autoscaler

                yield rq_workers

                for (queue_name, jobs) in get_jobs_by_queue(self.queue_class).items():
                    for (status, count) in jobs.items():
                        rq_jobs.add_metric([queue_name, status], count)

                yield rq_jobs

        logger.debug('RQ metrics collection finished')

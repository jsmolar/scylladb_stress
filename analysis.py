import concurrent.futures
import statistics
import threading
from dataclasses import dataclass
from datetime import datetime
from subprocess import CalledProcessError

from parser import StressResult, Parser
from runner import Runner


@dataclass
class StressRun:
    """Data class to store information about stress test run"""

    result: StressResult
    start_time: datetime
    end_time: datetime

    @property
    def duration(self) -> float:
        return (self.end_time - self.start_time).total_seconds()


def check_done(method):
    """Checks if the analysis is completed before calling `method`"""

    def wrapper(self, *args, **kwargs):
        if not self.done:
            raise ValueError(
                f"Cannot compute {method.__name__} because results are not available."
            )
        return method(self, *args, **kwargs)

    return wrapper


lock = threading.Lock()


class StressAnalysis:
    """Class that analyse Cassandra Stress Test"""

    def __init__(self):
        self.results = []
        self.done = False

    def _start_test(self, node_ip, duration):
        """Calls runner to run cassandra stress test and saves parsed results and time marks"""
        start_time = datetime.now()
        stress_result = Runner.run(node_ip, duration)
        end_time = datetime.now()

        result = StressRun(Parser(stress_result).parse(), start_time, end_time)
        with lock:
            self.results.append(result)

    def run(self, node_ip, nproc, process_runtime):
        """
        Runs stress tests in concurrent threads and waits until all threads terminates
        Args:
            :param node_ip: ip of a node from nodetool output
            :param nproc: number of concurrent stress test runs
            :param process_runtime: specify the time to run, in seconds, minutes or hours
        """

        with concurrent.futures.ThreadPoolExecutor(max_workers=nproc) as executor:
            futures = [
                executor.submit(self._start_test, node_ip, process_runtime[i])
                for i in range(nproc)
            ]

            # Wait for all threads to complete and check for exceptions
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()  # Raises any exception thrown in the thread
                except CalledProcessError as e:
                    print(f"Exception occurred in one of the threads: {e}")
                    raise

        self.done = True

    @property
    @check_done
    def op_rate_sum(self):
        return sum(result.result.op_rate for result in self.results)

    @property
    @check_done
    def avg_latency_mean(self):
        return statistics.mean(result.result.latency_mean for result in self.results)

    @property
    @check_done
    def avg_latency_99_percentile(self):
        return statistics.mean(
            result.result.latency_99th_percentile for result in self.results
        )

    @property
    @check_done
    def stdev_latency_max(self):
        return statistics.stdev(result.result.latency_max for result in self.results)

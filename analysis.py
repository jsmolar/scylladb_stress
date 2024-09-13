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


lock = threading.Lock()


class StressAnalysis:
    """Class that analyse Cassandra Stress Test"""

    def __init__(self):
        self.threads = []
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
        for i in range(nproc):
            t = threading.Thread(
                target=self._start_test,
                args=(
                    node_ip,
                    process_runtime[i],
                ),
            )
            self.threads.append(t)
            t.start()

        [t.join() for t in self.threads]
        self.done = True

    @property
    def op_rate_sum(self):
        if not self.done:
            raise ValueError(
                "Cannot compute op_rate_sum because results are not available."
            )
        return sum(result.result.op_rate for result in self.results)

    @property
    def avg_latency_mean(self):
        if not self.done:
            raise ValueError(
                "Cannot compute avg_latency_mean because results are not available."
            )
        return statistics.mean(result.result.latency_mean for result in self.results)

    @property
    def avg_latency_99_percentile(self):
        if not self.done:
            raise ValueError(
                "Cannot compute avg_latency_99_percentile because results are not available."
            )
        return statistics.mean(
            result.result.latency_99th_percentile for result in self.results
        )

    @property
    def stdev_latency_max(self):
        if not self.done:
            raise ValueError(
                "Cannot compute stdev_latency_max because results are not available."
            )
        return statistics.stdev(result.result.latency_max for result in self.results)
